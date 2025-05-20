# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

Set-StrictMode -Version Latest

$githubToken = $env:GITHUB_TOKEN
if ([string]::IsNullOrEmpty($githubToken)) {
    Write-Error "The GITHUB_TOKEN environment variable is not set or is empty. Please set it before running this script."
    exit 1
}

$netrcPath = Join-Path -Path $env:USERPROFILE -ChildPath ".netrc"
$netrcContent = @"
machine api.github.com
    password $githubToken
"@

try {
    $utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($netrcPath, $netrcContent, $utf8WithoutBom)
    Write-Host "Successfully wrote GitHub API configuration to: $netrcPath (UTF-8 without BOM)" -ForegroundColor Green
}
catch {
    Write-Error "Failed to write to $netrcPath. Error: $($_.Exception.Message)"
    exit 1
}