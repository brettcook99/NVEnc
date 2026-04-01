using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace NVEncBatchGui.Services;

public sealed class BatchAppState
{
    public string EncoderPath { get; set; } = string.Empty;

    public string OutputDirectory { get; set; } = string.Empty;

    public string ResolutionLabel { get; set; } = "240p";

    public string DecodeMode { get; set; } = nameof(DecodeModePreference.Auto);

    public bool IncludeAudio { get; set; } = true;

    public bool MaximizeSaturation { get; set; } = true;

    public int MaxConcurrentJobs { get; set; } = 12;

    public int MaxHardwareDecodeJobs { get; set; } = 4;

    public List<string> QueueInputs { get; set; } = new();

    public List<string> LogLines { get; set; } = new();
}

public static class BatchAppStateStore
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
    };

    private static string StateDirectory => Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "NVEncBatchGui");

    private static string StatePath => Path.Combine(StateDirectory, "batch-state.json");

    public static BatchAppState Load()
    {
        try
        {
            if (!File.Exists(StatePath))
            {
                return new BatchAppState();
            }

            var json = File.ReadAllText(StatePath);
            return JsonSerializer.Deserialize<BatchAppState>(json, SerializerOptions) ?? new BatchAppState();
        }
        catch (IOException)
        {
            return new BatchAppState();
        }
        catch (UnauthorizedAccessException)
        {
            return new BatchAppState();
        }
        catch (JsonException)
        {
            return new BatchAppState();
        }
    }

    public static void Save(BatchAppState state)
    {
        try
        {
            Directory.CreateDirectory(StateDirectory);
            File.WriteAllText(StatePath, JsonSerializer.Serialize(state, SerializerOptions));
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}