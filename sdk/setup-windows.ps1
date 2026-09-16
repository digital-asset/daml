# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

[CmdletBinding()]
param(
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$GitVersion = '2.55.0.5'
$GitTag = 'v2.55.0.windows.5'
$Sha256 = '5AA8A20F6E9ABB2C755F0E73C91C687701A46B309AD84A0CA6509380FA4AE290'
$Url = "https://github.com/git-for-windows/git/releases/download/$GitTag/PortableGit-$GitVersion-64-bit.7z.exe"
$MaxOutputBaseLength = 40

$RepoRoot = Split-Path -Parent $PSScriptRoot
$ShellDir = Join-Path (Split-Path -Parent $RepoRoot) ((Split-Path -Leaf $RepoRoot) + '_windows_bash')
$BashExe = Join-Path $ShellDir 'usr\bin\bash.exe'
$BashForBazel = ($BashExe -replace '\\', '/')

function Test-Shell {
    if (-not (Test-Path $BashExe)) { return $false }
    try {
        return (& $BashExe -c 'echo ready' 2>$null) -eq 'ready'
    } catch {
        return $false
    }
}

$changed = $false

if ((Test-Shell) -and -not $Force) {
    Write-Host "Shell already provisioned at $BashExe"
} else {
    $changed = $true
    if (Test-Path $ShellDir) {
        Write-Host "Removing previous $ShellDir"
        Remove-Item -Recurse -Force $ShellDir
    }

    $archive = Join-Path ([System.IO.Path]::GetTempPath()) "PortableGit-$GitVersion.7z.exe"
    if (-not (Test-Path $archive)) {
        Write-Host "Downloading PortableGit $GitVersion (56 MB)"
        $previous = $ProgressPreference
        $ProgressPreference = 'SilentlyContinue'
        try {
            Invoke-WebRequest -Uri $Url -OutFile $archive -UseBasicParsing
        } finally {
            $ProgressPreference = $previous
        }
    }

    $actual = (Get-FileHash -Path $archive -Algorithm SHA256).Hash
    if ($actual -ne $Sha256) {
        Remove-Item -Force $archive
        throw "sha256 mismatch for PortableGit $GitVersion`n  expected $Sha256`n  actual   $actual"
    }
    Write-Host "Verified sha256 $Sha256"

    Write-Host "Extracting to $ShellDir"
    & $archive "-o$ShellDir" -y | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "PortableGit extraction failed with exit code $LASTEXITCODE"
    }
    Remove-Item -Force $archive

    if (-not (Test-Shell)) {
        throw "Extraction completed but $BashExe does not run"
    }
    Write-Host "Extracted and verified $BashExe"
}

$readme = @"
# Windows build shell

PortableGit $GitVersion, provisioned by ``setup-windows.ps1`` for the SDK
checkout at $PSScriptRoot

Safe to delete. To recreate it and reset ``BAZEL_SH``:

    powershell -File $PSScriptRoot\setup-windows.ps1
"@

Set-Content -Path (Join-Path $ShellDir 'README.md') -Value $readme -Encoding UTF8

$existing = [Environment]::GetEnvironmentVariable('BAZEL_SH', 'User')
if ($existing -and $existing -ne $BashForBazel) {
    Write-Warning "Replacing existing user BAZEL_SH`n  was $existing`n  now $BashForBazel"
}

if ($existing -ne $BashForBazel) {
    [Environment]::SetEnvironmentVariable("BAZEL_SH", $BashForBazel, "User")
    Write-Host "Set user BAZEL_SH to $BashForBazel"
    $changed = $true
} else {
    Write-Host "User BAZEL_SH already correct"
}

$env:BAZEL_SH = $BashForBazel

$server = if ($changed) { & bazelisk.exe info server_pid 2>$null } else { $null }
if ($changed -and $LASTEXITCODE -eq 0 -and $server) {
    Write-Host "Shutting down the Bazel server so it picks up BAZEL_SH"
    & bazelisk.exe shutdown 2>&1 | Out-Null
}

$outputBase = (& bazelisk.exe info output_base 2>$null)
if ($LASTEXITCODE -eq 0 -and $outputBase) {
    $outputBase = $outputBase.Trim()
    if ($outputBase.Length -gt $MaxOutputBaseLength) {
        Write-Warning @"
Bazel's output base is $($outputBase.Length) characters long:
    $outputBase
This build generates paths about 210 characters below it, and Windows caps a
process path at 260. Builds fail with "The filename or extension is too long"
or "file doesn't exist" for files that are plainly there. Shorten it by adding
this line to .bazelrc.local:

    startup --output_base=C:/b
"@
    }
}

Write-Host ""
Write-Host "Done. This shell is ready; new terminals pick BAZEL_SH up automatically."
Write-Host "Build with: bazelisk build //..."
