using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.UI.Dispatching;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using NVEncBatchGui.Models;
using NVEncBatchGui.Services;
using Windows.Storage.Pickers;

namespace NVEncBatchGui;

public sealed partial class MainWindow : Window
{
    private const int DefaultMaxConcurrentJobs = 12;
    private const int DefaultMaxHardwareDecodeJobs = 4;
    private const int MaxConcurrentJobsLimit = 64;
    private static readonly TimeSpan LogRefreshInterval = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan SummaryRefreshInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan DeferredPersistInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan ProgressUiUpdateInterval = TimeSpan.FromMilliseconds(500);

    private readonly ObservableCollection<EncodeJob> _jobs = new();
    private readonly Dictionary<EncodeJob, NVEncProcessService> _activeServices = new();
    private readonly Queue<string> _logLines = new();
    private readonly ConcurrentQueue<string> _pendingLogLines = new();
    private readonly DispatcherQueueTimer _logRefreshTimer;
    private readonly DispatcherQueueTimer _summaryRefreshTimer;
    private readonly DispatcherQueueTimer _persistStateTimer;
    private CancellationTokenSource? _batchCancellation;
    private bool _isBatchRunning;
    private bool _restoringState;
    private bool _windowReady;
    private int _logRefreshRequested;
    private int _summaryRefreshRequested;

    private sealed record BatchRunContext(
        string EncoderPath,
        string ResolutionLabel,
        bool IncludeAudio,
        bool MaximizeSaturation,
        DecodeModePreference DecodeMode,
        int MaxConcurrentJobs,
        int MaxHardwareDecodeJobs,
        IReadOnlyList<EncodeJob> Jobs);

    public MainWindow()
    {
        InitializeComponent();
        _logRefreshTimer = DispatcherQueue.CreateTimer();
        _logRefreshTimer.Interval = LogRefreshInterval;
        _logRefreshTimer.IsRepeating = false;
        _logRefreshTimer.Tick += (_, _) => FlushPendingLogLines();

        _summaryRefreshTimer = DispatcherQueue.CreateTimer();
        _summaryRefreshTimer.Interval = SummaryRefreshInterval;
        _summaryRefreshTimer.IsRepeating = false;
        _summaryRefreshTimer.Tick += (_, _) => FlushSummaryRefresh();

        _persistStateTimer = DispatcherQueue.CreateTimer();
        _persistStateTimer.Interval = DeferredPersistInterval;
        _persistStateTimer.IsRepeating = false;
        _persistStateTimer.Tick += (_, _) => FlushPersistState();

        Closed += (_, _) => PersistState();

        JobsListView.ItemsSource = _jobs;
        HookSettingEvents();

        var (restoredJobs, missingJobs, restoredLogs) = RestoreState();
        if (EncoderPathTextBox.Text.Length == 0)
        {
            EncoderPathTextBox.Text = TryResolveEncoderPath();
        }
        if (OutputDirectoryTextBox.Text.Length == 0)
        {
            OutputDirectoryTextBox.Text = Path.Combine(FindRepoRoot() ?? AppContext.BaseDirectory, "output");
        }
        if (ResolutionComboBox.SelectedIndex < 0)
        {
            ResolutionComboBox.SelectedIndex = 0;
        }
        if (double.IsNaN(MaxConcurrentJobsNumberBox.Value) || MaxConcurrentJobsNumberBox.Value <= 0)
        {
            MaxConcurrentJobsNumberBox.Value = DefaultMaxConcurrentJobs;
        }
        if (double.IsNaN(MaxHardwareDecodeJobsNumberBox.Value) || MaxHardwareDecodeJobsNumberBox.Value < 0)
        {
            MaxHardwareDecodeJobsNumberBox.Value = DefaultMaxHardwareDecodeJobs;
        }

        RefreshPendingOutputPaths();
        UpdateStatusBanner();
        UpdateSummary();
        UpdateSchedulerDiagnostics();
        UpdateCommandState();
        UpdateCommandPreview();
        _windowReady = true;

        if (!restoredLogs)
        {
            AppendLog("GUI initialized. Build the forked NVEncC64.exe or browse to an existing binary to start encoding.");
            AppendLog($"Trace log: {BatchTraceLogger.CurrentLogPath}");
        }
        else if (restoredJobs > 0)
        {
            AppendLog($"Restored {restoredJobs} queued job(s) from the previous session.");
            AppendLog($"Trace log: {BatchTraceLogger.CurrentLogPath}");
        }
        else
        {
            AppendLog($"Trace log: {BatchTraceLogger.CurrentLogPath}");
        }

        if (missingJobs > 0)
        {
            AppendLog($"Skipped {missingJobs} missing input file(s) while restoring the previous session.");
        }
    }

    private string SelectedResolutionLabel => ((ResolutionComboBox.SelectedItem as ComboBoxItem)?.Content?.ToString()) ?? "240p";

    private DecodeModePreference SelectedDecodeMode => CommandLineBuilder.ParseDecodeModePreference((DecodeModeComboBox.SelectedItem as ComboBoxItem)?.Tag?.ToString());

    private bool IncludeAudio => IncludeAudioCheckBox.IsChecked == true;

    private bool MaximizeSaturation => SaturationCheckBox.IsChecked == true;

    private int MaxConcurrentJobs
    {
        get
        {
            var value = MaxConcurrentJobsNumberBox.Value;
            if (double.IsNaN(value))
            {
                return 1;
            }

            return Math.Max(1, Math.Min(MaxConcurrentJobsLimit, (int)Math.Round(value)));
        }
    }

    private int ConfiguredMaxHardwareDecodeJobs
    {
        get
        {
            var value = MaxHardwareDecodeJobsNumberBox.Value;
            if (double.IsNaN(value))
            {
                return 0;
            }

            return Math.Max(0, Math.Min(MaxConcurrentJobsLimit, (int)Math.Round(value)));
        }
    }

    private int EffectiveMaxHardwareDecodeJobs => Math.Min(MaxConcurrentJobs, ConfiguredMaxHardwareDecodeJobs);

    private static int NormalizeRestoredMaxConcurrentJobs(int value)
    {
        if (value <= 0)
        {
            return DefaultMaxConcurrentJobs;
        }

        // Migrate the previous hard cap/default of 3 to the new wider concurrency default.
        if (value == 3)
        {
            return DefaultMaxConcurrentJobs;
        }

        return Math.Max(1, Math.Min(MaxConcurrentJobsLimit, value));
    }

    private static int NormalizeRestoredMaxHardwareDecodeJobs(int value)
    {
        if (value < 0)
        {
            return DefaultMaxHardwareDecodeJobs;
        }

        return Math.Max(0, Math.Min(MaxConcurrentJobsLimit, value));
    }

    private static string NormalizeRestoredDecodeMode(string? decodeMode, string? resolutionLabel, int savedMaxConcurrentJobs)
    {
        var resolvedDecodeMode = CommandLineBuilder.ParseDecodeModePreference(decodeMode);
        var isLowResolutionTarget = string.Equals(resolutionLabel, "240p", StringComparison.OrdinalIgnoreCase)
            || string.Equals(resolutionLabel, "360p", StringComparison.OrdinalIgnoreCase);

        // Migrate the previous low-resolution hardware-decode throughput trial to Auto
        // so small outputs use the newer lower-VD default unless the user reselects Hardware.
        if (resolvedDecodeMode == DecodeModePreference.Hardware && isLowResolutionTarget && savedMaxConcurrentJobs == 3)
        {
            return nameof(DecodeModePreference.Auto);
        }

        return resolvedDecodeMode.ToString();
    }

    private void HookSettingEvents()
    {
        EncoderPathTextBox.LostFocus += (_, _) => HandleSettingChanged();
        OutputDirectoryTextBox.LostFocus += (_, _) => HandleSettingChanged(refreshPendingOutputs: true);
        IncludeAudioCheckBox.Checked += (_, _) => HandleSettingChanged();
        IncludeAudioCheckBox.Unchecked += (_, _) => HandleSettingChanged();
        DecodeModeComboBox.SelectionChanged += (_, _) => HandleSettingChanged();
        SaturationCheckBox.Checked += (_, _) => HandleSettingChanged();
        SaturationCheckBox.Unchecked += (_, _) => HandleSettingChanged();
        MaxConcurrentJobsNumberBox.ValueChanged += (_, _) => HandleSettingChanged();
        MaxHardwareDecodeJobsNumberBox.ValueChanged += (_, _) => HandleSettingChanged();
    }

    private async void BrowseEncoderButton_Click(object sender, RoutedEventArgs e)
    {
        var picker = new FileOpenPicker();
        picker.FileTypeFilter.Add(".exe");
        picker.SuggestedStartLocation = PickerLocationId.ComputerFolder;
        InitializePicker(picker);

        var file = await picker.PickSingleFileAsync();
        if (file is null)
        {
            return;
        }

        EncoderPathTextBox.Text = file.Path;
        UpdateStatusBanner();
        UpdateCommandState();
        UpdateCommandPreview();
        PersistState();
    }

    private async void BrowseOutputButton_Click(object sender, RoutedEventArgs e)
    {
        var picker = new FolderPicker();
        picker.FileTypeFilter.Add("*");
        picker.SuggestedStartLocation = PickerLocationId.VideosLibrary;
        InitializePicker(picker);

        var folder = await picker.PickSingleFolderAsync();
        if (folder is null)
        {
            return;
        }

        OutputDirectoryTextBox.Text = folder.Path;
        RefreshPendingOutputPaths();
        UpdateStatusBanner();
        UpdateCommandPreview();
        PersistState();
    }

    private async void AddVideosButton_Click(object sender, RoutedEventArgs e)
    {
        var picker = new FileOpenPicker();
        picker.SuggestedStartLocation = PickerLocationId.VideosLibrary;
        foreach (var extension in new[] { ".mp4", ".mkv", ".mov", ".avi", ".m2ts", ".ts", ".webm" })
        {
            picker.FileTypeFilter.Add(extension);
        }

        InitializePicker(picker);
        var files = await picker.PickMultipleFilesAsync();
        if (files is null || files.Count == 0)
        {
            return;
        }

        var existingInputs = new HashSet<string>(_jobs.Select(job => job.InputPath), StringComparer.OrdinalIgnoreCase);
        var addedCount = 0;
        foreach (var file in files)
        {
            if (existingInputs.Contains(file.Path))
            {
                continue;
            }

            _jobs.Add(new EncodeJob(file.Path, string.Empty));
            existingInputs.Add(file.Path);
            addedCount++;
        }

        RefreshPendingOutputPaths();
        UpdateSummary();
        UpdateCommandState();
        UpdateCommandPreview();
        PersistState();
        AppendLog($"Added {addedCount} input file(s) to the queue.");
    }

    private void RemoveSelectedButton_Click(object sender, RoutedEventArgs e)
    {
        var toRemove = JobsListView.SelectedItems.OfType<EncodeJob>().Where(job => job.CanRemove).ToList();
        foreach (var job in toRemove)
        {
            _jobs.Remove(job);
        }

        RefreshPendingOutputPaths();
        UpdateSummary();
        UpdateCommandState();
        UpdateCommandPreview();
        PersistState();
        if (toRemove.Count > 0)
        {
            AppendLog($"Removed {toRemove.Count} job(s) from the queue.");
        }
    }

    private void ClearCompletedButton_Click(object sender, RoutedEventArgs e)
    {
        var removable = _jobs.Where(job => job.State is EncodeJobState.Completed or EncodeJobState.Failed or EncodeJobState.Canceled).ToList();
        foreach (var job in removable)
        {
            _jobs.Remove(job);
        }

        RefreshPendingOutputPaths();
        UpdateSummary();
        UpdateCommandState();
        UpdateCommandPreview();
        PersistState();
    }

    private async void StartBatchButton_Click(object sender, RoutedEventArgs e)
    {
        if (_isBatchRunning)
        {
            return;
        }

        var encoderPath = EncoderPathTextBox.Text.Trim();
        if (!File.Exists(encoderPath))
        {
            UpdateStatusBanner("NVEncC executable is missing. Build NVEncC64.exe or browse to it before starting the batch.");
            return;
        }

        if (_jobs.Count == 0)
        {
            UpdateStatusBanner("Add one or more input videos before starting the batch.");
            return;
        }

        var outputDirectory = OutputDirectoryTextBox.Text.Trim();
        if (outputDirectory.Length == 0)
        {
            UpdateStatusBanner("Choose an output directory before starting the batch.");
            return;
        }

        Directory.CreateDirectory(outputDirectory);
        RefreshPendingOutputPaths();

        var batchRunContext = new BatchRunContext(
            encoderPath,
            SelectedResolutionLabel,
            IncludeAudio,
            MaximizeSaturation,
            SelectedDecodeMode,
            MaxConcurrentJobs,
            EffectiveMaxHardwareDecodeJobs,
            _jobs.ToList());

        _batchCancellation = new CancellationTokenSource();
        _isBatchRunning = true;
        UpdateCommandState();
        UpdateCommandPreview();
        UpdateStatusBanner($"Running {_jobs.Count(job => job.State != EncodeJobState.Completed)} queued job(s) with max concurrency {batchRunContext.MaxConcurrentJobs}.");
        AppendLog($"Starting batch with max concurrency {batchRunContext.MaxConcurrentJobs}. Target size {batchRunContext.ResolutionLabel}, audio {(batchRunContext.IncludeAudio ? "on" : "off")}, saturation {(batchRunContext.MaximizeSaturation ? "enabled" : "disabled")}, hardware decode cap {batchRunContext.MaxHardwareDecodeJobs}.");
        AppendLog($"Performance profile: {CommandLineBuilder.DescribeDecodeStrategy(batchRunContext.DecodeMode, batchRunContext.ResolutionLabel, batchRunContext.MaxHardwareDecodeJobs)}, {CommandLineBuilder.DescribeVideoProfile()}, split encode {(batchRunContext.MaximizeSaturation ? "auto/auto_forced" : "auto")}, and native JSON telemetry.");
        PersistState();

        try
        {
            await Task.Run(() => RunBatchAsync(batchRunContext, _batchCancellation.Token));
            UpdateStatusBanner("Batch finished. Review the queue and logs for per-job outcomes.");
        }
        catch (OperationCanceledException)
        {
            UpdateStatusBanner("Batch interrupted by user request.");
        }
        catch (Exception ex)
        {
            _batchCancellation?.Cancel();
            foreach (var service in SnapshotActiveServices())
            {
                service.Stop();
            }

            AppendLog($"Batch aborted due to an unexpected {ex.GetType().Name}: {ex.Message}");
            UpdateStatusBanner($"Batch failed to start or continue: {ex.Message}");
        }
        finally
        {
            _isBatchRunning = false;
            _batchCancellation?.Dispose();
            _batchCancellation = null;
            UpdateSummary();
            UpdateCommandState();
            UpdateCommandPreview();
            PersistState();
        }
    }

    private void StopBatchButton_Click(object sender, RoutedEventArgs e)
    {
        if (!_isBatchRunning)
        {
            return;
        }

        _batchCancellation?.Cancel();
        foreach (var service in SnapshotActiveServices())
        {
            service.Stop();
        }

        AppendLog("Stop requested. Signaling active NVEncC processes through their abort events.");
        UpdateStatusBanner("Stopping batch...");
        UpdateSchedulerDiagnostics("Scheduler: stopping active jobs after the user cancellation request.");
    }

    private void ResolutionComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_isBatchRunning || !_windowReady)
        {
            return;
        }

        RefreshPendingOutputPaths();
        AppendLog($"Target size changed to {SelectedResolutionLabel}.");
        UpdateCommandPreview();
        PersistState();
    }

    private void JobsListView_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!_windowReady)
        {
            return;
        }

        UpdateCommandState();
        UpdateCommandPreview();
    }

    private async Task RunBatchAsync(BatchRunContext batchRunContext, CancellationToken cancellationToken)
    {
        var stalledJobs = batchRunContext.Jobs.Where(job => job.State == EncodeJobState.Running).ToList();
        foreach (var job in stalledJobs)
        {
            job.PrepareForLaunch();
        }

        if (stalledJobs.Count > 0)
        {
            AppendLog($"Scheduler re-queued {stalledJobs.Count} job(s) that were still marked as running.");
        }

        var pendingJobs = new Queue<EncodeJob>(batchRunContext.Jobs.Where(job => job.State != EncodeJobState.Completed));
        var runningJobs = new Dictionary<EncodeJob, Task>();
        var hardwareDecodeJobs = new HashSet<EncodeJob>();
        BatchTraceLogger.Write("scheduler", $"Initialized batch loop. TotalJobs={batchRunContext.Jobs.Count}, Pending={pendingJobs.Count}, RequeuedRunning={stalledJobs.Count}, EncoderPath={batchRunContext.EncoderPath}");

        while (pendingJobs.Count > 0 || runningJobs.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            BatchTraceLogger.Write("scheduler", $"Loop tick. PendingQueue={pendingJobs.Count}, Running={runningJobs.Count}, ActiveJobs={string.Join(", ", runningJobs.Keys.Select(job => job.DisplayName))}");

            await DrainCompletedJobsAsync(runningJobs, hardwareDecodeJobs).ConfigureAwait(false);

            var schedulerDecision = DetermineDesiredConcurrency(pendingJobs.Count, runningJobs.Keys.ToList(), batchRunContext.MaximizeSaturation, batchRunContext.MaxConcurrentJobs);
            var desiredConcurrency = schedulerDecision.DesiredConcurrency;
            BatchTraceLogger.Write("scheduler", $"Decision desired={desiredConcurrency}/{batchRunContext.MaxConcurrentJobs}. {schedulerDecision.Diagnostics}");
            EnqueueUi(() => SchedulerDiagnosticsTextBlock.Text = schedulerDecision.Diagnostics);
            while (pendingJobs.Count > 0 && runningJobs.Count < desiredConcurrency)
            {
                var job = pendingJobs.Dequeue();
                var useSplitEncode = ShouldUseSplitEncode(pendingJobs.Count, runningJobs.Count, batchRunContext.MaximizeSaturation);
                var effectiveDecodeMode = CommandLineBuilder.ResolveDecodeModeForJob(batchRunContext.DecodeMode, batchRunContext.ResolutionLabel, hardwareDecodeJobs.Count, batchRunContext.MaxHardwareDecodeJobs);
                if (effectiveDecodeMode == DecodeModePreference.Hardware)
                {
                    hardwareDecodeJobs.Add(job);
                }

                AppendLog($"Scheduler launching {job.DisplayName} using {(useSplitEncode ? "single-job split encode" : "multi-job concurrency")} mode with {CommandLineBuilder.DescribeDecodeMode(effectiveDecodeMode, batchRunContext.ResolutionLabel)}.");
                BatchTraceLogger.Write("scheduler", $"Launching job {job.DisplayName}. RemainingPendingAfterDequeue={pendingJobs.Count}, RunningBeforeLaunch={runningJobs.Count}, SplitEncode={useSplitEncode}, EffectiveDecode={effectiveDecodeMode}, ActiveHardwareDecodeJobs={hardwareDecodeJobs.Count}/{batchRunContext.MaxHardwareDecodeJobs}, Output={job.OutputPath}");
                runningJobs[job] = RunSingleJobAsync(job, batchRunContext, effectiveDecodeMode, useSplitEncode, cancellationToken);
            }

            if (runningJobs.Count == 0)
            {
                BatchTraceLogger.Write("scheduler", "No active jobs remain after scheduling. Continuing loop.");
                continue;
            }

            var completionTask = Task.WhenAny(runningJobs.Values);
            var pollTask = Task.Delay(750, cancellationToken);
            var triggeredTask = await Task.WhenAny(completionTask, pollTask).ConfigureAwait(false);
            BatchTraceLogger.Write("scheduler", triggeredTask == completionTask
                ? "Wait loop woke due to job completion."
                : "Wait loop woke due to poll interval.");
            await DrainCompletedJobsAsync(runningJobs, hardwareDecodeJobs).ConfigureAwait(false);
        }

        BatchTraceLogger.Write("scheduler", "Batch loop exited cleanly. Pending and running job sets are empty.");
    }

    private static async Task DrainCompletedJobsAsync(Dictionary<EncodeJob, Task> runningJobs, ISet<EncodeJob> hardwareDecodeJobs)
    {
        foreach (var completedJob in runningJobs.Where(pair => pair.Value.IsCompleted).Select(pair => pair.Key).ToList())
        {
            await runningJobs[completedJob].ConfigureAwait(false);
            runningJobs.Remove(completedJob);
            hardwareDecodeJobs.Remove(completedJob);
            BatchTraceLogger.Write("scheduler", $"Removed completed task for {completedJob.DisplayName}. RunningRemaining={runningJobs.Count}");
        }
    }

    private async Task RunSingleJobAsync(EncodeJob job, BatchRunContext batchRunContext, DecodeModePreference effectiveDecodeMode, bool useSplitEncode, CancellationToken cancellationToken)
    {
        var service = new NVEncProcessService();
        NvEncResultSnapshot? finalResult = null;
        var lastProgressUiUpdateUtc = DateTimeOffset.MinValue;
        BatchTraceLogger.Write("job", $"Preparing job {job.DisplayName}. Input={job.InputPath}, Output={job.OutputPath}, SplitEncode={useSplitEncode}");
        lock (_activeServices)
        {
            _activeServices[job] = service;
        }

        service.ProgressReceived += snapshot => EnqueueUi(() =>
        {
            if (DateTimeOffset.UtcNow - lastProgressUiUpdateUtc < ProgressUiUpdateInterval)
            {
                return;
            }

            lastProgressUiUpdateUtc = DateTimeOffset.UtcNow;
            job.ApplySnapshot(snapshot);
            ScheduleSummaryRefresh();
        });

        service.ResultReceived += snapshot =>
        {
            finalResult = snapshot;
            EnqueueUi(() =>
            {
                job.ApplyResult(snapshot);
                ScheduleSummaryRefresh();
            });
        };

        service.LogReceived += line => AppendLog($"[{job.DisplayName}] {line}");

        EnqueueUi(() =>
        {
            job.PrepareForLaunch();
            job.MarkRunning();
            ScheduleSummaryRefresh();
        });

        try
        {
            var arguments = CommandLineBuilder.BuildArguments(job, batchRunContext.ResolutionLabel, batchRunContext.IncludeAudio, useSplitEncode, effectiveDecodeMode);
            AppendLog($"[{job.DisplayName}] {CommandLineBuilder.BuildDisplayCommand(batchRunContext.EncoderPath, arguments)}");
            BatchTraceLogger.Write("job", $"{job.DisplayName}: invoking NVEncC with {arguments.Count} arguments.");

            var exitCode = await service.RunAsync(batchRunContext.EncoderPath, arguments, cancellationToken).ConfigureAwait(false);
            BatchTraceLogger.Write("job", $"{job.DisplayName}: RunAsync returned exit code {exitCode}. CancellationRequested={cancellationToken.IsCancellationRequested}");
            EnqueueUi(() =>
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    job.MarkCanceled();
                    BatchTraceLogger.Write("job", $"{job.DisplayName}: marked canceled after RunAsync completion.");
                    return;
                }

                if (exitCode == 0)
                {
                    var outputBytes = File.Exists(job.OutputPath) ? new FileInfo(job.OutputPath).Length : 0;
                    job.MarkCompleted(outputBytes, finalResult);
                    AppendLog($"[{job.DisplayName}] {job.DetailMessage}.");
                    BatchTraceLogger.Write("job", $"{job.DisplayName}: marked completed. OutputBytes={outputBytes}, Detail={job.DetailMessage}");
                }
                else
                {
                    job.MarkFailed($"NVEncC exited with code {exitCode}.");
                    AppendLog($"[{job.DisplayName}] Failed with exit code {exitCode}.");
                    BatchTraceLogger.Write("job", $"{job.DisplayName}: marked failed with exit code {exitCode}.");
                }

                ScheduleSummaryRefresh();
                SchedulePersistState();
            });
        }
        catch (OperationCanceledException)
        {
            BatchTraceLogger.Write("job", $"{job.DisplayName}: caught OperationCanceledException.");
            EnqueueUi(() =>
            {
                job.MarkCanceled();
                ScheduleSummaryRefresh();
                SchedulePersistState();
            });
        }
        catch (Exception ex)
        {
            BatchTraceLogger.WriteException("job.exception", ex, $"{job.DisplayName}: unexpected exception while encoding.");
            EnqueueUi(() =>
            {
                job.MarkFailed(ex.Message);
                AppendLog($"[{job.DisplayName}] ERROR: {ex.Message}");
                ScheduleSummaryRefresh();
                SchedulePersistState();
            });
        }
        finally
        {
            lock (_activeServices)
            {
                _activeServices.Remove(job, out _);
            }

            service.Dispose();
            BatchTraceLogger.Write("job", $"{job.DisplayName}: service disposed. ActiveServicesRemaining={SnapshotActiveServices().Count}");
            EnqueueUi(() =>
            {
                ScheduleSummaryRefresh();
            });
        }
    }

    private IReadOnlyList<NVEncProcessService> SnapshotActiveServices()
    {
        lock (_activeServices)
        {
            return _activeServices.Values.ToList();
        }
    }

    private void RefreshPendingOutputPaths()
    {
        var outputDirectory = OutputDirectoryTextBox.Text.Trim();
        if (outputDirectory.Length == 0)
        {
            return;
        }

        Directory.CreateDirectory(outputDirectory);
        var reservedOutputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var job in _jobs.Where(job => job.State != EncodeJobState.Running))
        {
            var outputPath = CommandLineBuilder.ComposeOutputPath(job.InputPath, outputDirectory, SelectedResolutionLabel, reservedOutputs);
            reservedOutputs.Add(outputPath);
            job.OutputPath = outputPath;
        }

        UpdateCommandPreview();
    }

    private void UpdateSummary()
    {
        var totalJobs = _jobs.Count;
        var pendingJobs = _jobs.Count(job => job.State == EncodeJobState.Pending);
        var runningJobs = _jobs.Count(job => job.State == EncodeJobState.Running);
        var completedJobs = _jobs.Count(job => job.State == EncodeJobState.Completed);
        var failedJobs = _jobs.Count(job => job.State == EncodeJobState.Failed);
        var canceledJobs = _jobs.Count(job => job.State == EncodeJobState.Canceled);
        var aggregateFps = _jobs.Where(job => job.State == EncodeJobState.Running).Sum(job => job.Fps);
        var aggregateBitrate = _jobs.Where(job => job.State == EncodeJobState.Running).Sum(job => job.BitrateKbps);
        var maxGpu = _jobs.Where(job => job.State == EncodeJobState.Running).Select(job => job.GpuUsage).DefaultIfEmpty(0).Max();
        var maxVe = _jobs.Where(job => job.State == EncodeJobState.Running).Select(job => job.VideoEncoderUsage).DefaultIfEmpty(0).Max();
        var maxVd = _jobs.Where(job => job.State == EncodeJobState.Running).Select(job => job.VideoDecoderUsage).DefaultIfEmpty(0).Max();

        TotalJobsValueText.Text = totalJobs.ToString();
        JobStateSummaryText.Text = $"{runningJobs} running / {pendingJobs} pending / {completedJobs} complete / {failedJobs} failed / {canceledJobs} canceled";
        AggregateFpsValueText.Text = aggregateFps.ToString("F2");
        AggregateBitrateValueText.Text = $"{aggregateBitrate:N0} kbps";
        AggregateGpuValueText.Text = runningJobs == 0 ? "idle" : $"{maxGpu}% / VE {maxVe}% / VD {maxVd}%";
        UpdateSchedulerDiagnostics();
    }

    private void ScheduleSummaryRefresh()
    {
        if (Interlocked.Exchange(ref _summaryRefreshRequested, 1) != 0)
        {
            return;
        }

        EnqueueUi(() =>
        {
            if (!_summaryRefreshTimer.IsRunning)
            {
                _summaryRefreshTimer.Start();
            }
        });
    }

    private void FlushSummaryRefresh()
    {
        _summaryRefreshTimer.Stop();
        UpdateSummary();
        Interlocked.Exchange(ref _summaryRefreshRequested, 0);
    }

    private void UpdateCommandState()
    {
        var encoderExists = File.Exists(EncoderPathTextBox.Text.Trim());
        EncoderPathTextBox.IsEnabled = !_isBatchRunning;
        OutputDirectoryTextBox.IsEnabled = !_isBatchRunning;
        ResolutionComboBox.IsEnabled = !_isBatchRunning;
        DecodeModeComboBox.IsEnabled = !_isBatchRunning;
        MaxHardwareDecodeJobsNumberBox.IsEnabled = !_isBatchRunning;
        IncludeAudioCheckBox.IsEnabled = !_isBatchRunning;
        SaturationCheckBox.IsEnabled = !_isBatchRunning;
        MaxConcurrentJobsNumberBox.IsEnabled = !_isBatchRunning;
        AddVideosButton.IsEnabled = !_isBatchRunning;
        RemoveSelectedButton.IsEnabled = !_isBatchRunning && JobsListView.SelectedItems.OfType<EncodeJob>().Any(job => job.CanRemove);
        ClearCompletedButton.IsEnabled = !_isBatchRunning && _jobs.Any(job => job.State is EncodeJobState.Completed or EncodeJobState.Failed or EncodeJobState.Canceled);
        StartBatchButton.IsEnabled = !_isBatchRunning && encoderExists && _jobs.Count > 0;
        StopBatchButton.IsEnabled = _isBatchRunning;
    }

    private void UpdateStatusBanner(string? overrideText = null)
    {
        if (!string.IsNullOrWhiteSpace(overrideText))
        {
            StatusBannerTextBlock.Text = overrideText;
            return;
        }

        var encoderPath = EncoderPathTextBox.Text.Trim();
        if (encoderPath.Length == 0)
        {
            StatusBannerTextBlock.Text = "NVEncC executable has not been resolved yet. Browse to NVEncC64.exe or build the native project first.";
            return;
        }

        StatusBannerTextBlock.Text = File.Exists(encoderPath)
            ? $"Ready. Using encoder at {encoderPath}. Queue will launch up to {MaxConcurrentJobs} concurrent H.264 jobs with {CommandLineBuilder.DescribeDecodeStrategy(SelectedDecodeMode, SelectedResolutionLabel, EffectiveMaxHardwareDecodeJobs)}, {CommandLineBuilder.DescribeVideoProfile()}, native JSON telemetry, and adaptive split-encode saturation."
            : $"Encoder path is set to {encoderPath}, but the file does not exist. Build NVEncC64.exe or browse to a valid binary.";
    }

    private void UpdateSchedulerDiagnostics(string? overrideText = null)
    {
        if (!string.IsNullOrWhiteSpace(overrideText))
        {
            SchedulerDiagnosticsTextBlock.Text = overrideText;
            return;
        }

        var pendingJobs = _jobs.Count(job => job.State == EncodeJobState.Pending);
        var runningJobs = _jobs.Where(job => job.State == EncodeJobState.Running).ToList();
        SchedulerDiagnosticsTextBlock.Text = DetermineDesiredConcurrency(pendingJobs, runningJobs, MaximizeSaturation, MaxConcurrentJobs).Diagnostics;
    }

    private void AppendLog(string message)
    {
        var stamped = $"{DateTime.Now:HH:mm:ss} {message}";
        _pendingLogLines.Enqueue(stamped);
        BatchTraceLogger.Write("ui", message);

        if (Interlocked.Exchange(ref _logRefreshRequested, 1) != 0)
        {
            return;
        }

        EnqueueUi(() =>
        {
            if (!_logRefreshTimer.IsRunning)
            {
                _logRefreshTimer.Start();
            }
        });
    }

    private void FlushPendingLogLines()
    {
        _logRefreshTimer.Stop();
        while (_pendingLogLines.TryDequeue(out var line))
        {
            _logLines.Enqueue(line);
            while (_logLines.Count > 400)
            {
                _logLines.Dequeue();
            }
        }

        LogTextBox.Text = string.Join(Environment.NewLine, _logLines);
        Interlocked.Exchange(ref _logRefreshRequested, 0);
        SchedulePersistState();

        if (!_pendingLogLines.IsEmpty && Interlocked.Exchange(ref _logRefreshRequested, 1) == 0)
        {
            _logRefreshTimer.Start();
        }
    }

    private void UpdateCommandPreview()
    {
        var previewJob = JobsListView.SelectedItems.OfType<EncodeJob>().FirstOrDefault()
            ?? _jobs.FirstOrDefault(job => job.State == EncodeJobState.Pending)
            ?? _jobs.FirstOrDefault();

        if (previewJob is null)
        {
            CommandPreviewTextBox.Text = "Add a video to preview the generated NVEncC command.";
            return;
        }

        var encoderPath = EncoderPathTextBox.Text.Trim();
        if (encoderPath.Length == 0)
        {
            encoderPath = "NVEncC64.exe";
        }

        var previewDecodeMode = CommandLineBuilder.ResolveDecodeModeForJob(SelectedDecodeMode, SelectedResolutionLabel, 0, EffectiveMaxHardwareDecodeJobs);
        var arguments = CommandLineBuilder.BuildArguments(previewJob, SelectedResolutionLabel, IncludeAudio, ShouldUseSplitEncodeForPreview(previewJob), previewDecodeMode);
        CommandPreviewTextBox.Text = CommandLineBuilder.BuildDisplayCommand(encoderPath, arguments);
    }

    private void HandleSettingChanged(bool refreshPendingOutputs = false)
    {
        if (_restoringState)
        {
            return;
        }

        if (!_isBatchRunning && refreshPendingOutputs)
        {
            RefreshPendingOutputPaths();
        }

        UpdateStatusBanner();
        UpdateSchedulerDiagnostics();
        UpdateCommandState();
        UpdateCommandPreview();
        PersistState();
    }

    private (int RestoredJobs, int MissingJobs, bool RestoredLogs) RestoreState()
    {
        _restoringState = true;
        try
        {
            var state = BatchAppStateStore.Load();
            EncoderPathTextBox.Text = state.EncoderPath;
            OutputDirectoryTextBox.Text = state.OutputDirectory;
            SelectResolution(state.ResolutionLabel);
            SelectDecodeMode(NormalizeRestoredDecodeMode(state.DecodeMode, state.ResolutionLabel, state.MaxConcurrentJobs));
            IncludeAudioCheckBox.IsChecked = state.IncludeAudio;
            SaturationCheckBox.IsChecked = state.MaximizeSaturation;
            MaxConcurrentJobsNumberBox.Value = NormalizeRestoredMaxConcurrentJobs(state.MaxConcurrentJobs);
            MaxHardwareDecodeJobsNumberBox.Value = NormalizeRestoredMaxHardwareDecodeJobs(state.MaxHardwareDecodeJobs);

            var (restoredJobs, missingJobs) = RestoreQueuedJobs(state.QueueInputs);
            var restoredLogs = RestoreLogLines(state.LogLines);
            return (restoredJobs, missingJobs, restoredLogs);
        }
        finally
        {
            _restoringState = false;
        }
    }

    private void SelectResolution(string? resolutionLabel)
    {
        var selectedItem = ResolutionComboBox.Items
            .OfType<ComboBoxItem>()
            .FirstOrDefault(item => string.Equals(item.Content?.ToString(), resolutionLabel, StringComparison.OrdinalIgnoreCase));
        ResolutionComboBox.SelectedItem = selectedItem ?? ResolutionComboBox.Items.OfType<ComboBoxItem>().FirstOrDefault();
    }

    private void SelectDecodeMode(string? decodeMode)
    {
        var selectedItem = DecodeModeComboBox.Items
            .OfType<ComboBoxItem>()
            .FirstOrDefault(item => string.Equals(item.Tag?.ToString(), decodeMode, StringComparison.OrdinalIgnoreCase));
        DecodeModeComboBox.SelectedItem = selectedItem ?? DecodeModeComboBox.Items.OfType<ComboBoxItem>().FirstOrDefault();
    }

    private (int RestoredJobs, int MissingJobs) RestoreQueuedJobs(IEnumerable<string> queueInputs)
    {
        var restoredJobs = 0;
        var missingJobs = 0;
        var existingInputs = new HashSet<string>(_jobs.Select(job => job.InputPath), StringComparer.OrdinalIgnoreCase);
        foreach (var inputPath in queueInputs.Where(path => !string.IsNullOrWhiteSpace(path)).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (!File.Exists(inputPath))
            {
                missingJobs++;
                continue;
            }

            if (!existingInputs.Add(inputPath))
            {
                continue;
            }

            _jobs.Add(new EncodeJob(inputPath, string.Empty));
            restoredJobs++;
        }
        return (restoredJobs, missingJobs);
    }

    private bool RestoreLogLines(IEnumerable<string> logLines)
    {
        var restored = false;
        _logLines.Clear();
        foreach (var line in logLines.Where(line => !string.IsNullOrWhiteSpace(line)).TakeLast(400))
        {
            _logLines.Enqueue(line);
            restored = true;
        }

        LogTextBox.Text = string.Join(Environment.NewLine, _logLines);
        return restored;
    }

    private void PersistState()
    {
        if (_restoringState)
        {
            return;
        }

        if (!DispatcherQueue.HasThreadAccess)
        {
            EnqueueUi(PersistState);
            return;
        }

        PersistStateCore();
    }

    private void SchedulePersistState()
    {
        if (_restoringState)
        {
            return;
        }

        EnqueueUi(() =>
        {
            _persistStateTimer.Stop();
            _persistStateTimer.Start();
        });
    }

    private void FlushPersistState()
    {
        _persistStateTimer.Stop();
        PersistStateCore();
    }

    private void PersistStateCore()
    {
        if (_restoringState)
        {
            return;
        }

        BatchAppStateStore.Save(new BatchAppState
        {
            EncoderPath = EncoderPathTextBox.Text.Trim(),
            OutputDirectory = OutputDirectoryTextBox.Text.Trim(),
            ResolutionLabel = SelectedResolutionLabel,
            DecodeMode = SelectedDecodeMode.ToString(),
            IncludeAudio = IncludeAudio,
            MaximizeSaturation = MaximizeSaturation,
            MaxConcurrentJobs = MaxConcurrentJobs,
            MaxHardwareDecodeJobs = ConfiguredMaxHardwareDecodeJobs,
            QueueInputs = _jobs
                .Where(job => job.State != EncodeJobState.Completed)
                .Select(job => job.InputPath)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList(),
            LogLines = _logLines.ToList(),
        });
    }

    private static int CalculateBurstIncrement(int runningCount, int queuedBacklog, int maxConcurrentJobs)
    {
        if (queuedBacklog <= 0)
        {
            return 0;
        }

        if (runningCount <= 0)
        {
            return Math.Min(Math.Min(4, queuedBacklog), maxConcurrentJobs);
        }

        if (runningCount < 4)
        {
            return Math.Min(4 - runningCount, queuedBacklog);
        }

        return Math.Min(Math.Min(runningCount, queuedBacklog), maxConcurrentJobs - runningCount);
    }

    private (int DesiredConcurrency, string Diagnostics) DetermineDesiredConcurrency(int queuedBacklog, IReadOnlyCollection<EncodeJob> runningJobs, bool maximizeSaturation, int maxConcurrentJobs)
    {
        if (runningJobs.Count == 0)
        {
            var desired = maximizeSaturation
                ? Math.Min(maxConcurrentJobs, Math.Min(4, queuedBacklog))
                : Math.Min(maxConcurrentJobs, queuedBacklog);
            return maximizeSaturation
                ? (desired, $"Scheduler preview: desired {desired}/{maxConcurrentJobs}. It opens with up to 4 jobs, then ramps in bursts while avg VE < 82%, avg GPU < 92%, max VE < 95%, and max GPU < 98%.")
                : (desired, $"Scheduler preview: desired {desired}/{maxConcurrentJobs}. Saturation mode is off, so the queue can immediately fill available slots when backlog exists.");
        }

        if (!maximizeSaturation)
        {
            var desired = Math.Min(maxConcurrentJobs, runningJobs.Count + queuedBacklog);
            return (desired, $"Scheduler: desired {desired}/{maxConcurrentJobs}. Saturation mode is off, so concurrency follows backlog with {runningJobs.Count} running and {queuedBacklog} queued.");
        }

        if (queuedBacklog == 0 || runningJobs.Count >= maxConcurrentJobs)
        {
            var reason = queuedBacklog == 0
                ? "No queued jobs remain, so the scheduler is holding current work only."
                : $"The configured cap of {maxConcurrentJobs} concurrent job(s) is already in use.";
            return (runningJobs.Count, $"Scheduler: holding {runningJobs.Count}/{maxConcurrentJobs}. {reason}");
        }

        var telemetryReadyJobs = runningJobs.Where(job => job.HasLiveTelemetry).ToList();
        if (telemetryReadyJobs.Count == 0)
        {
            var oldestRunning = runningJobs
                .Where(job => job.StartedAt.HasValue)
                .Select(job => DateTimeOffset.Now - job.StartedAt!.Value)
                .DefaultIfEmpty(TimeSpan.Zero)
                .Max();
            var canProbe = oldestRunning >= TimeSpan.FromSeconds(1.5);
            var burst = CalculateBurstIncrement(runningJobs.Count, queuedBacklog, maxConcurrentJobs);
            var desired = canProbe
                ? Math.Min(maxConcurrentJobs, runningJobs.Count + burst)
                : runningJobs.Count;
            var diagnostics = canProbe
                ? $"Scheduler: raising desired concurrency to {desired}/{maxConcurrentJobs}. Live telemetry is not ready yet, but the oldest active job has run for {oldestRunning.TotalSeconds:F1}s, so another burst of {burst} job(s) is opening as a probe."
                : $"Scheduler: holding {desired}/{maxConcurrentJobs} while waiting for live telemetry. Oldest active job age is {oldestRunning.TotalSeconds:F1}s; burst probing begins after 1.5s.";
            return (desired, diagnostics);
        }

        var avgGpu = telemetryReadyJobs.Average(job => job.GpuUsage);
        var avgVe = telemetryReadyJobs.Average(job => job.VideoEncoderUsage > 0 ? job.VideoEncoderUsage : job.GpuUsage);
        var maxGpu = telemetryReadyJobs.Max(job => job.GpuUsage);
        var maxVe = telemetryReadyJobs.Max(job => job.VideoEncoderUsage > 0 ? job.VideoEncoderUsage : job.GpuUsage);

        if (avgVe < 82 && avgGpu < 92 && maxVe < 95 && maxGpu < 98)
        {
            var burst = CalculateBurstIncrement(runningJobs.Count, queuedBacklog, maxConcurrentJobs);
            var desired = Math.Min(maxConcurrentJobs, runningJobs.Count + burst);
            return (desired, $"Scheduler: raising desired concurrency to {desired}/{maxConcurrentJobs}. Live load is avg GPU {avgGpu:F0}% / VE {avgVe:F0}% with peaks GPU {maxGpu:F0}% / VE {maxVe:F0}%, so another burst of {burst} job(s) is opening.");
        }

        return (runningJobs.Count, $"Scheduler: holding {runningJobs.Count}/{maxConcurrentJobs}. Live load is avg GPU {avgGpu:F0}% / VE {avgVe:F0}% with peaks GPU {maxGpu:F0}% / VE {maxVe:F0}%, so the adaptive thresholds say to avoid opening another slot.");
    }

    private static bool ShouldUseSplitEncode(int queuedAfterLaunch, int activeBeforeLaunch, bool maximizeSaturation)
    {
        return maximizeSaturation && activeBeforeLaunch == 0 && queuedAfterLaunch == 0;
    }

    private bool ShouldUseSplitEncodeForPreview(EncodeJob previewJob)
    {
        return MaximizeSaturation
            && _jobs.Count(job => !ReferenceEquals(job, previewJob) && job.State != EncodeJobState.Completed) == 0;
    }

    private void EnqueueUi(Action action)
    {
        if (DispatcherQueue.HasThreadAccess)
        {
            action();
            return;
        }

        DispatcherQueue.TryEnqueue(() => action());
    }

    private void InitializePicker(object picker)
    {
        WinRT.Interop.InitializeWithWindow.Initialize(picker, WinRT.Interop.WindowNative.GetWindowHandle(this));
    }

    private string TryResolveEncoderPath()
    {
        var repoRoot = FindRepoRoot();
        if (repoRoot is null)
        {
            return string.Empty;
        }

        var preferredCandidates = new[]
        {
            Path.Combine(repoRoot, "NVEncC", "x64", "RelStatic", "NVEncC64.exe"),
            Path.Combine(repoRoot, "NVEncC", "x64", "Release", "NVEncC64.exe"),
            Path.Combine(repoRoot, "NVEncC64.exe"),
        };

        foreach (var candidate in preferredCandidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return Directory.EnumerateFiles(repoRoot, "NVEncC64.exe", SearchOption.AllDirectories).FirstOrDefault() ?? string.Empty;
    }

    private string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "NVEnc.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}