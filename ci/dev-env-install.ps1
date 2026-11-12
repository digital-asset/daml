# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

Write-Host "TODO: ci/dev-env-install.ps1 only exists because release-trigger's Windows job has no tool setup; remove it once release-trigger installs bazelisk, jq and gcloud itself"

$BazeliskVersion = '1.29.0'
$BazeliskSha256 = '092a8738d5b41aae7a85c42cc961b1034e3389aba43ffc20c0fabda7b43e095b'
$JqVersion = '1.7.1'
$JqSha256 = '7451fbbf37feffb9bf262bd97c54f0da558c63f0748e64152dd87b0a07b6d6ab'
$GcloudVersion = '592.0.0'
$GcloudSha256 = '65002e0d679fc2ade00163b97637f45656d6fd5250d6f5962e4a1a555f2628ea'

$binDir = Join-Path $env:USERPROFILE '.local\bin'
$gcloudRoot = Join-Path $env:USERPROFILE ".local\google-cloud-sdk-$GcloudVersion"
$gcloudCompleteMarker = Join-Path $gcloudRoot '.installed'

function Get-VerifiedFile {
    param([string] $Url, [string] $Sha256, [string] $OutFile)
    Invoke-WebRequest -Uri $Url -OutFile $OutFile -UseBasicParsing
    $actual = (Get-FileHash -Algorithm SHA256 -Path $OutFile).Hash.ToLower()
    if ($actual -ne $Sha256) {
        Remove-Item -Force $OutFile
        throw "sha256 mismatch for $Url`n  expected $Sha256`n  actual   $actual"
    }
}

New-Item -ItemType Directory -Force -Path $binDir | Out-Null

$bazeliskDownload = Join-Path $env:TEMP "bazelisk-$BazeliskVersion.exe"
Get-VerifiedFile "https://github.com/bazelbuild/bazelisk/releases/download/v$BazeliskVersion/bazelisk-windows-amd64.exe" $BazeliskSha256 $bazeliskDownload
Move-Item -Force -Path $bazeliskDownload -Destination (Join-Path $binDir 'bazelisk.exe')
Copy-Item -Force -Path (Join-Path $binDir 'bazelisk.exe') -Destination (Join-Path $binDir 'bazel.exe')

$jqDownload = Join-Path $env:TEMP "jq-$JqVersion.exe"
Get-VerifiedFile "https://github.com/jqlang/jq/releases/download/jq-$JqVersion/jq-windows-amd64.exe" $JqSha256 $jqDownload
Move-Item -Force -Path $jqDownload -Destination (Join-Path $binDir 'jq.exe')

if (Test-Path $gcloudCompleteMarker) {
    Write-Host "gcloud $GcloudVersion already installed in $gcloudRoot"
} else {
    $extractDir = "$gcloudRoot.partial"
    foreach ($dir in @($gcloudRoot, $extractDir)) {
        if (Test-Path $dir) { Remove-Item -Recurse -Force $dir }
    }
    $gcloudDownload = Join-Path $env:TEMP "google-cloud-sdk-$GcloudVersion.zip"
    Get-VerifiedFile "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-$GcloudVersion-windows-x86_64-bundled-python.zip" $GcloudSha256 $gcloudDownload
    Expand-Archive -Path $gcloudDownload -DestinationPath $extractDir
    Move-Item -Path (Join-Path $extractDir 'google-cloud-sdk') -Destination $gcloudRoot
    Remove-Item -Recurse -Force $extractDir
    Remove-Item -Force $gcloudDownload
    New-Item -ItemType File -Path $gcloudCompleteMarker | Out-Null
}

Write-Host "##vso[task.setvariable variable=CLOUDSDK_PYTHON]$(Join-Path $gcloudRoot 'platform\bundledpython\python.exe')"
Write-Host "##vso[task.prependpath]$(Join-Path $gcloudRoot 'bin')"
Write-Host "##vso[task.prependpath]$binDir"
