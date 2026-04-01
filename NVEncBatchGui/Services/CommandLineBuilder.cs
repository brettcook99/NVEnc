using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NVEncBatchGui.Models;

namespace NVEncBatchGui.Services;

public enum DecodeModePreference
{
    Auto,
    Hardware,
    Software,
}

public static class CommandLineBuilder
{
    private const int AutoSoftwareDecodePixelThreshold = 640 * 360;

    private static readonly IReadOnlyDictionary<string, (int Width, int Height)> ResolutionPresets =
        new Dictionary<string, (int Width, int Height)>(StringComparer.OrdinalIgnoreCase)
        {
            ["240p"] = (426, 240),
            ["360p"] = (640, 360),
            ["480p"] = (854, 480),
            ["720p"] = (1280, 720),
        };

    public static string DescribeVideoProfile()
    {
        return "codec h264, preset p7, vbrhq 5M";
    }

    public static DecodeModePreference ResolveDecodeModePreference(DecodeModePreference decodeMode, string resolutionLabel)
    {
        var size = ResolveTargetSize(resolutionLabel);
        return ResolveDecodeMode(decodeMode, size);
    }

    public static DecodeModePreference ResolveDecodeModeForJob(DecodeModePreference decodeMode, string resolutionLabel, int activeHardwareDecodeJobs, int maxHardwareDecodeJobs)
    {
        var effectiveDecodeMode = ResolveDecodeModePreference(decodeMode, resolutionLabel);
        if (effectiveDecodeMode != DecodeModePreference.Hardware)
        {
            return effectiveDecodeMode;
        }

        return activeHardwareDecodeJobs < Math.Max(0, maxHardwareDecodeJobs)
            ? DecodeModePreference.Hardware
            : DecodeModePreference.Software;
    }

    public static string DescribeDecodeStrategy(DecodeModePreference decodeMode, string resolutionLabel, int maxHardwareDecodeJobs)
    {
        var effectiveDecodeMode = ResolveDecodeModePreference(decodeMode, resolutionLabel);
        if (effectiveDecodeMode != DecodeModePreference.Hardware)
        {
            return DescribeDecodeMode(decodeMode, resolutionLabel);
        }

        if (maxHardwareDecodeJobs <= 0)
        {
            return decodeMode == DecodeModePreference.Auto
                ? "auto (software decode because max hardware decode jobs is 0)"
                : "hardware requested, but max hardware decode jobs is 0 so jobs fall back to software decode";
        }

        return decodeMode == DecodeModePreference.Auto
            ? $"auto (hardware decode for up to {maxHardwareDecodeJobs} concurrent job(s), then software fallback)"
            : $"hardware decode for up to {maxHardwareDecodeJobs} concurrent job(s), then software fallback";
    }

    private static (int Width, int Height) ResolveTargetSize(string resolutionLabel)
    {
        if (!ResolutionPresets.TryGetValue(resolutionLabel, out var size))
        {
            size = ResolutionPresets["240p"];
        }

        return size;
    }

    public static IReadOnlyList<string> BuildArguments(EncodeJob job, string resolutionLabel, bool includeAudio, bool useSplitEncode, DecodeModePreference decodeMode)
    {
        var size = ResolveTargetSize(resolutionLabel);

        var effectiveDecodeMode = ResolveDecodeMode(decodeMode, size);
        var useSoftwareDecode = effectiveDecodeMode == DecodeModePreference.Software;

        var arguments = new List<string>
        {
            useSoftwareDecode ? "--avsw" : "--avhw",
            "-i",
            job.InputPath,
            "-o",
            job.OutputPath,
            "--codec",
            "h264",
            "--preset",
            "p7",
            "--vbrhq",
            "5M",
            "--split-enc",
            useSplitEncode ? "auto_forced" : "auto",
            "--telemetry-json",
            "stdout",
            "--output-res",
            $"{size.Width}x{size.Height},preserve_aspect_ratio=decrease",
        };

        if (includeAudio)
        {
            arguments.Add("--audio-copy");
        }

        if (useSplitEncode)
        {
            arguments.Add("--output-thread");
            arguments.Add("1");
        }

        return arguments;
    }

    public static DecodeModePreference ParseDecodeModePreference(string? value)
    {
        return Enum.TryParse<DecodeModePreference>(value, true, out var decodeMode)
            ? decodeMode
            : DecodeModePreference.Auto;
    }

    public static string DescribeDecodeMode(DecodeModePreference decodeMode, string resolutionLabel)
    {
        var effectiveDecodeMode = ResolveDecodeModePreference(decodeMode, resolutionLabel);
        return decodeMode switch
        {
            DecodeModePreference.Auto => $"auto ({GetDecodeModeLabel(effectiveDecodeMode)})",
            _ => GetDecodeModeLabel(effectiveDecodeMode),
        };
    }

    private static DecodeModePreference ResolveDecodeMode(DecodeModePreference decodeMode, (int Width, int Height) size)
    {
        return decodeMode switch
        {
            DecodeModePreference.Hardware => DecodeModePreference.Hardware,
            DecodeModePreference.Software => DecodeModePreference.Software,
            _ => ShouldUseSoftwareDecode(size) ? DecodeModePreference.Software : DecodeModePreference.Hardware,
        };
    }

    private static string GetDecodeModeLabel(DecodeModePreference decodeMode)
    {
        return decodeMode switch
        {
            DecodeModePreference.Hardware => "hardware decode",
            DecodeModePreference.Software => "software decode",
            _ => "auto",
        };
    }

    private static bool ShouldUseSoftwareDecode((int Width, int Height) size)
    {
        return size.Width * size.Height <= AutoSoftwareDecodePixelThreshold;
    }

    public static string ComposeOutputPath(string inputPath, string outputDirectory, string resolutionLabel, ISet<string>? reservedOutputs = null)
    {
        var safeResolution = string.IsNullOrWhiteSpace(resolutionLabel) ? "240p" : resolutionLabel.Trim().ToLowerInvariant();
        var fileStem = Path.GetFileNameWithoutExtension(inputPath);
        var attempt = 0;

        while (true)
        {
            var suffix = attempt == 0 ? string.Empty : $"_{attempt:00}";
            var candidate = Path.Combine(outputDirectory, $"{fileStem}_{safeResolution}{suffix}.mp4");
            if ((reservedOutputs == null || !reservedOutputs.Contains(candidate)) && !File.Exists(candidate))
            {
                return candidate;
            }

            attempt++;
        }
    }

    public static string BuildDisplayCommand(string encoderPath, IReadOnlyList<string> arguments)
    {
        return Quote(encoderPath) + " " + string.Join(" ", arguments.Select(Quote));
    }

    private static string Quote(string value)
    {
        return value.Any(char.IsWhiteSpace) ? $"\"{value.Replace("\"", "\\\"")}\"" : value;
    }
}