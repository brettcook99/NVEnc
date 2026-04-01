[CmdletBinding()]
param(
    [string]$Version = "20250830",
    [string]$RepoRoot = "",
    [switch]$Force,
    [switch]$SkipRuntimeStage
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$archiveName = "ffmpeg_dlls_for_hwenc_${Version}.7z"
$downloadUrl = "https://github.com/rigaya/ffmpeg_dlls_for_hwenc/releases/download/$Version/$archiveName"
$archivePath = Join-Path $RepoRoot "ffmpeg_lgpl.7z"
$extractPath = Join-Path $RepoRoot "ffmpeg_lgpl"
$runtimeSource = Join-Path $extractPath "lib\x64"
$requiredRuntimeDlls = @(
    "avcodec-62.dll",
    "avformat-62.dll",
    "avutil-60.dll",
    "avfilter-11.dll",
    "swresample-6.dll",
    "avdevice-62.dll"
)

function Resolve-SevenZipPath {
    $command = Get-Command "7z.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($command) {
        return $command.Source
    }

    foreach ($candidate in @(
        (Join-Path $env:ProgramFiles "7-Zip\7z.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "7-Zip\7z.exe")
    )) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    throw "7z.exe was not found. Install 7-Zip or place 7z.exe on PATH, then rerun this script."
}

function Test-ExtractionReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path (Join-Path $Path "include"))) {
        return $false
    }

    foreach ($dll in $requiredRuntimeDlls) {
        if (-not (Test-Path (Join-Path $runtimeSource $dll))) {
            return $false
        }
    }

    return $true
}

if ($Force -and (Test-Path $extractPath)) {
    Remove-Item $extractPath -Recurse -Force
}

if ($Force -or -not (Test-Path $archivePath)) {
    Write-Host "Downloading $archiveName from $downloadUrl"
    Invoke-WebRequest -Uri $downloadUrl -OutFile $archivePath
}

if ($Force -or -not (Test-ExtractionReady -Path $extractPath)) {
    if (Test-Path $extractPath) {
        Remove-Item $extractPath -Recurse -Force
    }

    $sevenZip = Resolve-SevenZipPath
    Write-Host "Extracting $archivePath to $extractPath"
    & $sevenZip x "-o$extractPath" -y $archivePath | Out-Host
}

if (-not (Test-ExtractionReady -Path $extractPath)) {
    throw "ffmpeg_lgpl extraction is incomplete. Expected include files and FFmpeg 62 runtime DLLs under $extractPath."
}

$stagedTargets = New-Object System.Collections.Generic.List[string]
if (-not $SkipRuntimeStage) {
    $buildRoot = Join-Path $RepoRoot "_build\x64"
    if (Test-Path $buildRoot) {
        foreach ($targetDirectory in Get-ChildItem $buildRoot -Directory) {
            foreach ($dll in $requiredRuntimeDlls) {
                Copy-Item (Join-Path $runtimeSource $dll) (Join-Path $targetDirectory.FullName $dll) -Force
            }
            $stagedTargets.Add($targetDirectory.FullName)
        }
    }
}

Write-Host "ffmpeg_lgpl is ready at $extractPath"
if ($stagedTargets.Count -eq 0) {
    Write-Host "No existing x64 build output directories were found. Future NVEncC builds will copy DLLs from ffmpeg_lgpl\\lib\\x64 automatically."
} else {
    Write-Host "Staged runtime DLLs into:"
    foreach ($target in $stagedTargets) {
        Write-Host "  $target"
    }
}