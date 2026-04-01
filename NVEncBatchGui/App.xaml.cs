using System;
using System.IO;
using Microsoft.UI.Xaml;
using NVEncBatchGui.Services;

namespace NVEncBatchGui;

public partial class App : Application
{
    private Window? _window;

    public App()
    {
        InitializeComponent();
        UnhandledException += OnUnhandledException;
    }

    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        BatchTraceLogger.Write("app", $"Application launched. SessionLog={BatchTraceLogger.SessionLogPath}");
        _window = new MainWindow();
        _window.Activate();
    }

    private void OnUnhandledException(object sender, Microsoft.UI.Xaml.UnhandledExceptionEventArgs args)
    {
        try
        {
            BatchTraceLogger.WriteException("app.unhandled", args.Exception, args.Message);
            var directory = BatchTraceLogger.LogsDirectory;
            Directory.CreateDirectory(directory);
            var logPath = Path.Combine(directory, "unhandled-exception.log");
            File.AppendAllText(logPath,
                $"[{DateTimeOffset.Now:O}] {args.Message}{Environment.NewLine}{args.Exception}{Environment.NewLine}{Environment.NewLine}");
        }
        catch
        {
        }
    }
}