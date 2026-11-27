# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

param([string] $Command)

[Console]::Error.WriteLine("TODO: sdk/dev-env/windows/bin/dadew.ps1 is a placeholder for release-trigger's 'Upload Bazel metrics' step; ci/dev-env-install.ps1 puts jq and gcloud on PATH instead. Remove it once release-trigger no longer calls it")

switch ($Command) {
    'enable' { $env:PATH = "$PSScriptRoot;$env:PATH" }
    'where' { Write-Output $PSScriptRoot }
}
