# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

Set-Location (Join-Path $PSScriptRoot '..')

$test_suite = '//compatibility'
$bazel_args = @($args)
if (($bazel_args.length -ge 1) -and ($bazel_args[0] -eq '--quick')) {
  $test_suite = '//compatibility:quick'
  $bazel_args = @($bazel_args | Select-Object -Skip 1)
}

function bazelisk() {
    Write-Output ">> bazelisk $args"
    $global:lastexitcode = 0
    $backupErrorActionPreference = $script:ErrorActionPreference
    $script:ErrorActionPreference = 'Continue'
    & bazelisk.exe @args 2>&1 | %{ "$_" }
    $script:ErrorActionPreference = $backupErrorActionPreference
    if ($global:lastexitcode -ne 0 -And $args[0] -ne 'shutdown') {
        Write-Output "<< bazelisk $args (failed, exit code: $global:lastexitcode)"
        throw ("Bazel returned non-zero exit code: $global:lastexitcode")
    }
    Write-Output "<< bazelisk $args (ok)"
}

$backupErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$sdk_targets = @(& bazelisk.exe query 'kind(alias, @compat_sdks//:all)' 2>$null | Where-Object { $_ })
$ErrorActionPreference = $backupErrorActionPreference
if (-not $sdk_targets) { throw 'Could not find any @compat_sdks targets to prebuild' }

foreach ($target in $sdk_targets) {
    bazelisk build $target @bazel_args
    bazelisk shutdown
}

bazelisk build $test_suite @bazel_args
bazelisk shutdown

bazelisk test $test_suite @bazel_args
