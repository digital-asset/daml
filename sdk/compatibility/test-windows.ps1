Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build the release artifacts required for running the compatibility
# tests against HEAD. At the moment this includes the SDK release tarball
# and the ledger-api-test-tool fat JAR.

$test_args = ""
if (($args.length -ge 1) -and ($args[0] -eq "--quick")) {
  $test_args = "--test_tag_filters=+head-quick"
}
write-output $test_args

# For reasons I do not understand, it seems to be important
# to sync here even though we sync in the previous step already.
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable


if (Test-Path -Path $env:appdata\stack\pantry\hackage\hackage-security-lock) {
    Write-Output ">> Nuking stack directory"
    Remove-Item -ErrorAction Continue -Force -Recurse -Path $env:appdata\stack
}

# This is currently shared between various powershell scripts.
# We should probably factor this out.
function bazel() {
    Write-Output ">> bazel $args"
    $global:lastexitcode = 0
    $backupErrorActionPreference = $script:ErrorActionPreference
    $script:ErrorActionPreference = "Continue"
    & bazel.exe @args 2>&1 | %{ "$_" }
    $script:ErrorActionPreference = $backupErrorActionPreference
    if ($global:lastexitcode -ne 0 -And $args[0] -ne "shutdown") {
        Write-Output "<< bazel $args (failed, exit code: $global:lastexitcode)"
        throw ("Bazel returned non-zero exit code: $global:lastexitcode")
    }
    Write-Output "<< bazel $args (ok)"
}

cd compatibility
# Symlinks don’t work on Windows.
cp ../.bazelrc .bazelrc

bazel shutdown
bazel fetch @nodejs_dev_env//...
bazel build //...
bazel shutdown

bazel test //... $test_args
