using System;
using System.IO;
using System.Text;

namespace NVEncBatchGui.Services;

public static class BatchTraceLogger
{
    private static readonly object Sync = new();
    private static string? _sessionLogPath;

    public static string LogsDirectory => GetLogsDirectory();

    public static string CurrentLogPath
    {
        get
        {
            lock (Sync)
            {
                EnsureSessionLocked();
                return Path.Combine(GetLogsDirectory(), "current.log");
            }
        }
    }

    public static string SessionLogPath
    {
        get
        {
            lock (Sync)
            {
                EnsureSessionLocked();
                return _sessionLogPath!;
            }
        }
    }

    public static void Write(string category, string message)
    {
        try
        {
            lock (Sync)
            {
                EnsureSessionLocked();
                var line = $"[{DateTimeOffset.Now:O}] [{category}] {message}{Environment.NewLine}";
                File.AppendAllText(_sessionLogPath!, line, Encoding.UTF8);
                File.AppendAllText(CurrentLogPath, line, Encoding.UTF8);
            }
        }
        catch
        {
        }
    }

    public static void WriteException(string category, Exception exception, string? context = null)
    {
        var message = context is null
            ? exception.ToString()
            : $"{context}{Environment.NewLine}{exception}";
        Write(category, message);
    }

    private static void EnsureSessionLocked()
    {
        if (_sessionLogPath is not null)
        {
            return;
        }

        var logsDirectory = GetLogsDirectory();
        Directory.CreateDirectory(logsDirectory);

        _sessionLogPath = Path.Combine(logsDirectory, $"session-{DateTimeOffset.Now:yyyyMMdd-HHmmss-fff}.log");
        var banner = $"[{DateTimeOffset.Now:O}] [trace] Log session started.{Environment.NewLine}";
        File.WriteAllText(_sessionLogPath, banner, Encoding.UTF8);
        File.WriteAllText(Path.Combine(logsDirectory, "current.log"), banner, Encoding.UTF8);
    }

    private static string GetLogsDirectory()
    {
        return Path.Combine(AppContext.BaseDirectory, "logs");
    }
}