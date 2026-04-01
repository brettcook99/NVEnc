using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.CompilerServices;
using NVEncBatchGui.Services;

namespace NVEncBatchGui.Models;

public enum EncodeJobState
{
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
}

public sealed class EncodeJob : INotifyPropertyChanged
{
    private string _outputPath;
    private EncodeJobState _state;
    private string _detailMessage;
    private double _progressPercent;
    private int _frameOut;
    private int _frameTotal;
    private double _fps;
    private int _bitrateKbps;
    private int _gpuUsage;
    private int _videoEncoderUsage;
    private int _videoDecoderUsage;
    private double _estimatedSizeMb;
    private TimeSpan? _remaining;
    private DateTimeOffset? _startedAt;
    private DateTimeOffset? _completedAt;

    public EncodeJob(string inputPath, string outputPath)
    {
        InputPath = inputPath;
        _outputPath = outputPath;
        _state = EncodeJobState.Pending;
        _detailMessage = "Queued";
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    public string InputPath { get; }

    public string OutputPath
    {
        get => _outputPath;
        set
        {
            if (SetProperty(ref _outputPath, value))
            {
                OnPropertyChanged(nameof(OutputFileName));
            }
        }
    }

    public string DisplayName => Path.GetFileName(InputPath);

    public string OutputFileName => Path.GetFileName(OutputPath);

    public EncodeJobState State
    {
        get => _state;
        private set
        {
            if (SetProperty(ref _state, value))
            {
                OnPropertyChanged(nameof(StateLabel));
                OnPropertyChanged(nameof(CanRemove));
            }
        }
    }

    public string StateLabel => State switch
    {
        EncodeJobState.Pending => "Queued",
        EncodeJobState.Running => "Running",
        EncodeJobState.Completed => "Completed",
        EncodeJobState.Failed => "Failed",
        EncodeJobState.Canceled => "Canceled",
        _ => "Unknown",
    };

    public bool CanRemove => State != EncodeJobState.Running;

    public bool HasLiveTelemetry => FrameOut > 0 || Fps > 0 || ProgressPercent > 0;

    public DateTimeOffset? StartedAt => _startedAt;

    public string DetailMessage
    {
        get => _detailMessage;
        private set => SetProperty(ref _detailMessage, value);
    }

    public double ProgressPercent
    {
        get => _progressPercent;
        private set
        {
            if (SetProperty(ref _progressPercent, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public int FrameOut
    {
        get => _frameOut;
        private set
        {
            if (SetProperty(ref _frameOut, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public int FrameTotal
    {
        get => _frameTotal;
        private set
        {
            if (SetProperty(ref _frameTotal, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public double Fps
    {
        get => _fps;
        private set
        {
            if (SetProperty(ref _fps, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public int BitrateKbps
    {
        get => _bitrateKbps;
        private set
        {
            if (SetProperty(ref _bitrateKbps, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public int GpuUsage
    {
        get => _gpuUsage;
        private set
        {
            if (SetProperty(ref _gpuUsage, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public int VideoEncoderUsage
    {
        get => _videoEncoderUsage;
        private set
        {
            if (SetProperty(ref _videoEncoderUsage, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public int VideoDecoderUsage
    {
        get => _videoDecoderUsage;
        private set
        {
            if (SetProperty(ref _videoDecoderUsage, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public double EstimatedSizeMb
    {
        get => _estimatedSizeMb;
        private set
        {
            if (SetProperty(ref _estimatedSizeMb, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public TimeSpan? Remaining
    {
        get => _remaining;
        private set
        {
            if (SetProperty(ref _remaining, value))
            {
                OnPropertyChanged(nameof(ProgressLine));
            }
        }
    }

    public string ProgressLine
    {
        get
        {
            var frameTotalText = FrameTotal > 0 ? FrameTotal.ToString() : "?";
            var remainText = Remaining.HasValue ? $" | remain {Remaining.Value:hh\\:mm\\:ss}" : string.Empty;
            var sizeText = EstimatedSizeMb > 0 ? $" | est {EstimatedSizeMb:F1} MB" : string.Empty;
            return $"{FrameOut}/{frameTotalText} frames | {Fps:F2} fps | {BitrateKbps} kbps | GPU {GpuUsage}% | VE {VideoEncoderUsage}% | VD {VideoDecoderUsage}%{remainText}{sizeText}";
        }
    }

    public void PrepareForLaunch()
    {
        State = EncodeJobState.Pending;
        DetailMessage = "Queued";
        ProgressPercent = 0;
        FrameOut = 0;
        FrameTotal = 0;
        Fps = 0;
        BitrateKbps = 0;
        GpuUsage = 0;
        VideoEncoderUsage = 0;
        VideoDecoderUsage = 0;
        EstimatedSizeMb = 0;
        Remaining = null;
        _startedAt = null;
        _completedAt = null;
    }

    public void MarkRunning()
    {
        State = EncodeJobState.Running;
        DetailMessage = "Encoding started";
        _startedAt = DateTimeOffset.Now;
        _completedAt = null;
    }

    public void ApplySnapshot(NvEncProgressSnapshot snapshot)
    {
        ProgressPercent = snapshot.Percent;
        FrameOut = snapshot.FrameOut;
        FrameTotal = snapshot.FrameTotal;
        Fps = snapshot.Fps;
        BitrateKbps = snapshot.BitrateKbps;
        GpuUsage = snapshot.GpuUsage ?? GpuUsage;
        VideoEncoderUsage = snapshot.VideoEncoderUsage ?? VideoEncoderUsage;
        VideoDecoderUsage = snapshot.VideoDecoderUsage ?? VideoDecoderUsage;
        EstimatedSizeMb = snapshot.EstimatedSizeMb ?? EstimatedSizeMb;
        Remaining = snapshot.Remaining;
        DetailMessage = "Receiving live encoder telemetry";
    }

    public void ApplyResult(NvEncResultSnapshot snapshot)
    {
        FrameOut = snapshot.FrameOut;
        FrameTotal = snapshot.FrameTotal > 0 ? snapshot.FrameTotal : FrameTotal;
        Fps = snapshot.Fps;
        BitrateKbps = snapshot.BitrateKbps;
        GpuUsage = snapshot.GpuUsage ?? GpuUsage;
        VideoEncoderUsage = snapshot.VideoEncoderUsage ?? VideoEncoderUsage;
        VideoDecoderUsage = snapshot.VideoDecoderUsage ?? VideoDecoderUsage;
        if (snapshot.OutputSizeBytes > 0)
        {
            EstimatedSizeMb = snapshot.OutputSizeBytes / 1024d / 1024d;
        }
    }

    public void MarkCompleted(long outputFileSizeBytes, NvEncResultSnapshot? result = null)
    {
        State = EncodeJobState.Completed;
        ProgressPercent = 100;
        Remaining = TimeSpan.Zero;
        _completedAt = DateTimeOffset.Now;

        if (result.HasValue)
        {
            ApplyResult(result.Value);
        }

        var effectiveOutputSizeBytes = result?.OutputSizeBytes > 0 ? result.Value.OutputSizeBytes : outputFileSizeBytes;
        if (effectiveOutputSizeBytes > 0)
        {
            EstimatedSizeMb = effectiveOutputSizeBytes / 1024d / 1024d;
        }

        var elapsed = result?.Elapsed ?? ((_startedAt.HasValue ? _completedAt - _startedAt : null) ?? TimeSpan.Zero);
        var detailParts = new List<string>
        {
            $"Completed in {elapsed:hh\\:mm\\:ss}"
        };
        if (Fps > 0)
        {
            detailParts.Add($"{Fps:F2} fps");
        }
        if (BitrateKbps > 0)
        {
            detailParts.Add($"{BitrateKbps:N0} kbps");
        }
        if (EstimatedSizeMb > 0)
        {
            detailParts.Add($"{EstimatedSizeMb:F1} MB");
        }
        if (GpuUsage > 0 || VideoEncoderUsage > 0)
        {
            detailParts.Add($"GPU {GpuUsage}% / VE {VideoEncoderUsage}%");
        }
        DetailMessage = string.Join(" | ", detailParts);
    }

    public void MarkFailed(string message)
    {
        State = EncodeJobState.Failed;
        _completedAt = DateTimeOffset.Now;
        DetailMessage = message;
    }

    public void MarkCanceled()
    {
        State = EncodeJobState.Canceled;
        _completedAt = DateTimeOffset.Now;
        DetailMessage = "Batch interrupted";
    }

    private bool SetProperty<T>(ref T storage, T value, [CallerMemberName] string? propertyName = null)
    {
        if (Equals(storage, value))
        {
            return false;
        }

        storage = value;
        OnPropertyChanged(propertyName);
        return true;
    }

    private void OnPropertyChanged([CallerMemberName] string? propertyName = null)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}