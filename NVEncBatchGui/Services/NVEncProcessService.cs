using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace NVEncBatchGui.Services;

public readonly record struct NvEncProgressSnapshot(
    double Percent,
    int FrameOut,
    int FrameTotal,
    double Fps,
    int BitrateKbps,
    TimeSpan? Remaining,
    int? GpuUsage,
    int? VideoEncoderUsage,
    int? VideoDecoderUsage,
    double? EstimatedSizeMb);

public readonly record struct NvEncResultSnapshot(
    int FrameOut,
    int FrameTotal,
    double Fps,
    int BitrateKbps,
    TimeSpan Elapsed,
    long OutputSizeBytes,
    double? CpuUsage,
    int? GpuUsage,
    int? VideoEncoderUsage,
    int? VideoDecoderUsage);

public static class NvEncTelemetryParser
{
    private const string Prefix = "NVENCC_TELEMETRY_JSON ";

    public static bool TryParseProgress(string line, out NvEncProgressSnapshot snapshot)
    {
        if (!TryGetRoot(line, out var root) || !string.Equals(GetOptionalString(root, "event"), "progress", StringComparison.OrdinalIgnoreCase))
        {
            snapshot = default;
            return false;
        }

        snapshot = new NvEncProgressSnapshot(
            GetOptionalDouble(root, "percent") ?? 0,
            GetOptionalInt(root, "frameOut") ?? 0,
            GetOptionalInt(root, "frameTotal") ?? 0,
            GetOptionalDouble(root, "fps") ?? 0,
            GetOptionalInt(root, "bitrateKbps") ?? 0,
            GetOptionalDouble(root, "remainingSeconds") is double remainingSeconds ? TimeSpan.FromSeconds(remainingSeconds) : null,
            GetOptionalInt(root, "gpu"),
            GetOptionalInt(root, "videoEncoder"),
            GetOptionalInt(root, "videoDecoder"),
            GetOptionalDouble(root, "estimatedSizeMb"));

        return true;
    }

    public static bool TryParseResult(string line, out NvEncResultSnapshot snapshot)
    {
        if (!TryGetRoot(line, out var root) || !string.Equals(GetOptionalString(root, "event"), "result", StringComparison.OrdinalIgnoreCase))
        {
            snapshot = default;
            return false;
        }

        snapshot = new NvEncResultSnapshot(
            GetOptionalInt(root, "frameOut") ?? 0,
            GetOptionalInt(root, "frameTotal") ?? 0,
            GetOptionalDouble(root, "fps") ?? 0,
            GetOptionalInt(root, "bitrateKbps") ?? 0,
            TimeSpan.FromSeconds(GetOptionalDouble(root, "elapsedSeconds") ?? 0),
            GetOptionalLong(root, "outputSizeBytes") ?? 0,
            GetOptionalDouble(root, "cpuUsage"),
            GetOptionalInt(root, "gpu"),
            GetOptionalInt(root, "videoEncoder"),
            GetOptionalInt(root, "videoDecoder"));

        return true;
    }

    private static bool TryGetRoot(string line, out JsonElement root)
    {
        root = default;
        if (!line.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(line[Prefix.Length..]);
            root = document.RootElement.Clone();
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string? GetOptionalString(JsonElement root, string propertyName)
    {
        return root.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String
            ? property.GetString()
            : null;
    }

    private static double? GetOptionalDouble(JsonElement root, string propertyName)
    {
        return root.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.Number
            ? property.GetDouble()
            : null;
    }

    private static int? GetOptionalInt(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.Number)
        {
            return null;
        }

        if (property.TryGetInt32(out var integerValue))
        {
            return integerValue;
        }

        return (int)Math.Round(property.GetDouble());
    }

    private static long? GetOptionalLong(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.Number)
        {
            return null;
        }

        if (property.TryGetInt64(out var integerValue))
        {
            return integerValue;
        }

        return (long)Math.Round(property.GetDouble());
    }
}

public static partial class NvEncProgressParser
{
    [GeneratedRegex(@"^\[(?<percent>\d+(?:\.\d+)?)%\]\s*(?<frameOut>\d+)(?:/(?<frameTotal>\d+))?\s*frames:\s*(?<fps>\d+(?:\.\d+)?)\s*fps,\s*(?<kbps>\d+)\s*kbps", RegexOptions.IgnoreCase)]
    private static partial Regex HeaderRegex();

    [GeneratedRegex(@"remain\s*(?<hh>\d+):(?<mm>\d+):(?<ss>\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex RemainRegex();

    [GeneratedRegex(@"GPU\s*(?<value>\d+)%", RegexOptions.IgnoreCase)]
    private static partial Regex GpuRegex();

    [GeneratedRegex(@"(?:VE|MFX)\s*(?<value>\d+)%", RegexOptions.IgnoreCase)]
    private static partial Regex VideoEncoderRegex();

    [GeneratedRegex(@"VD\s*(?<value>\d+)%", RegexOptions.IgnoreCase)]
    private static partial Regex VideoDecoderRegex();

    [GeneratedRegex(@"est out size\s*(?<value>\d+(?:\.\d+)?)MB", RegexOptions.IgnoreCase)]
    private static partial Regex EstimatedSizeRegex();

    public static bool TryParse(string line, out NvEncProgressSnapshot snapshot)
    {
        var header = HeaderRegex().Match(line);
        if (!header.Success)
        {
            snapshot = default;
            return false;
        }

        var percent = double.Parse(header.Groups["percent"].Value);
        var frameOut = int.Parse(header.Groups["frameOut"].Value);
        var frameTotal = header.Groups["frameTotal"].Success ? int.Parse(header.Groups["frameTotal"].Value) : 0;
        var fps = double.Parse(header.Groups["fps"].Value);
        var bitrate = int.Parse(header.Groups["kbps"].Value);

        TimeSpan? remaining = null;
        var remain = RemainRegex().Match(line);
        if (remain.Success)
        {
            remaining = new TimeSpan(
                int.Parse(remain.Groups["hh"].Value),
                int.Parse(remain.Groups["mm"].Value),
                int.Parse(remain.Groups["ss"].Value));
        }

        snapshot = new NvEncProgressSnapshot(
            percent,
            frameOut,
            frameTotal,
            fps,
            bitrate,
            remaining,
            ParseOptionalInt(GpuRegex().Match(line)),
            ParseOptionalInt(VideoEncoderRegex().Match(line)),
            ParseOptionalInt(VideoDecoderRegex().Match(line)),
            ParseOptionalDouble(EstimatedSizeRegex().Match(line)));

        return true;
    }

    private static int? ParseOptionalInt(Match match)
    {
        return match.Success ? int.Parse(match.Groups["value"].Value) : null;
    }

    private static double? ParseOptionalDouble(Match match)
    {
        return match.Success ? double.Parse(match.Groups["value"].Value) : null;
    }
}

public sealed class NVEncProcessService : IDisposable
{
    private const int GracefulStopTimeoutMs = 2000;
    private readonly object _processGate = new();
    private Process? _process;
    private int _stopRequested;

    public event Action<string>? LogReceived;

    public event Action<NvEncProgressSnapshot>? ProgressReceived;

    public event Action<NvEncResultSnapshot>? ResultReceived;

    public async Task<int> RunAsync(string encoderPath, IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        if (!File.Exists(encoderPath))
        {
            throw new FileNotFoundException("NVEncC executable was not found.", encoderPath);
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = encoderPath,
            WorkingDirectory = Path.GetDirectoryName(encoderPath) ?? AppContext.BaseDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true,
            CreateNoWindow = true,
        };

        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        var process = new Process
        {
            StartInfo = startInfo,
            EnableRaisingEvents = true,
        };

        try
        {
            lock (_processGate)
            {
                _process = process;
            }

            _stopRequested = 0;
            process.Start();
            LogReceived?.Invoke($"Started NVEncC pid {process.Id}.");

            using var stopRegistration = cancellationToken.Register(Stop);
            var stderrTask = PumpAsync(process.StandardError, allowLegacyProgress: true, cancellationToken);
            var stdoutTask = PumpAsync(process.StandardOutput, allowLegacyProgress: false, cancellationToken);

            await process.WaitForExitAsync().ConfigureAwait(false);
            await Task.WhenAll(stderrTask, stdoutTask).ConfigureAwait(false);
            LogReceived?.Invoke($"NVEncC pid {process.Id} exited with code {process.ExitCode}.");

            return process.ExitCode;
        }
        finally
        {
            lock (_processGate)
            {
                if (ReferenceEquals(_process, process))
                {
                    _process = null;
                }
            }

            try
            {
                process.Dispose();
            }
            catch (InvalidOperationException)
            {
            }
        }
    }

    public void Stop()
    {
        if (Interlocked.Exchange(ref _stopRequested, 1) != 0)
        {
            return;
        }

        lock (_processGate)
        {
            if (HasExitedOrUnavailableLocked())
            {
                return;
            }

            if (TrySignalAbortEvent(_process.Id))
            {
                LogReceived?.Invoke($"Requested graceful stop via NVEncC abort event for pid {_process.Id}.");
                _ = Task.Run(async () =>
                {
                    await Task.Delay(GracefulStopTimeoutMs).ConfigureAwait(false);
                    lock (_processGate)
                    {
                        if (HasExitedOrUnavailableLocked())
                        {
                            return;
                        }

                        TryKillProcessLocked($"Graceful stop timed out for pid {_process.Id}; forcing process termination.");
                    }
                });
                return;
            }

            LogReceived?.Invoke($"Abort event was unavailable for pid {_process.Id}; forcing process termination.");
            TryKillProcessLocked();
        }
    }

    public void Dispose()
    {
        Stop();
        lock (_processGate)
        {
            try
            {
                _process?.Dispose();
            }
            catch (InvalidOperationException)
            {
            }

            _process = null;
        }
    }

    private bool HasExitedOrUnavailableLocked()
    {
        if (_process is null)
        {
            return true;
        }

        try
        {
            return _process.HasExited;
        }
        catch (InvalidOperationException)
        {
            return true;
        }
    }

    private async Task PumpAsync(StreamReader reader, bool allowLegacyProgress, CancellationToken cancellationToken)
    {
        var rented = ArrayPool<char>.Shared.Rent(512);
        var builder = new StringBuilder();

        try
        {
            while (true)
            {
                int read;
                try
                {
                    read = await reader.ReadAsync(rented.AsMemory(0, rented.Length), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (read == 0)
                {
                    break;
                }

                for (var index = 0; index < read; index++)
                {
                    var character = rented[index];
                    if (character == '\r' || character == '\n')
                    {
                        FlushPending(builder, allowLegacyProgress);
                        continue;
                    }

                    builder.Append(character);
                }
            }

            FlushPending(builder, allowLegacyProgress);
        }
        finally
        {
            ArrayPool<char>.Shared.Return(rented);
        }
    }

    private void FlushPending(StringBuilder builder, bool allowLegacyProgress)
    {
        if (builder.Length == 0)
        {
            return;
        }

        var text = builder.ToString().Trim();
        builder.Clear();

        if (text.Length == 0)
        {
            return;
        }

        if (NvEncTelemetryParser.TryParseProgress(text, out var telemetryProgress))
        {
            ProgressReceived?.Invoke(telemetryProgress);
            return;
        }

        if (NvEncTelemetryParser.TryParseResult(text, out var telemetryResult))
        {
            ResultReceived?.Invoke(telemetryResult);
            return;
        }

        if (allowLegacyProgress && NvEncProgressParser.TryParse(text, out var legacyProgress))
        {
            ProgressReceived?.Invoke(legacyProgress);
            return;
        }

        LogReceived?.Invoke(text);
    }

    private static bool TrySignalAbortEvent(int processId)
    {
        try
        {
            using var abortEvent = EventWaitHandle.OpenExisting($"NVEncC_abort_{processId}");
            return abortEvent.Set();
        }
        catch (WaitHandleCannotBeOpenedException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private void TryKillProcessLocked(string? reason = null)
    {
        if (reason is not null)
        {
            LogReceived?.Invoke(reason);
        }

        try
        {
            _process?.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
        }
        catch (NotSupportedException)
        {
        }
    }
}