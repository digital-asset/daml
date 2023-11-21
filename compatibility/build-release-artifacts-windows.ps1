Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build the release artifacts required for running the compatibility
# tests against HEAD. At the moment this includes the SDK release tarball
# and the ledger-api-test-tool fat JAR.

# See https://github.com/lukesampson/scoop/issues/3859
Set-Strictmode -Off
.\dev-env\windows\bin\dadew.ps1 install
Set-StrictMode -Version latest
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable

if (Test-Path -Path $env:appdata\stack\pantry\hackage\hackage-security-lock) {
    Write-Output ">> Nuking stack directory"
    Remove-Item -ErrorAction Continue -Force -Recurse -Path $env:appdata\stack
}

$ARTIFACT_DIRS = if ("$env:BUILD_ARTIFACTSTAGINGDIRECTORY") { $env:BUILD_ARTIFACTSTAGINGDIRECTORY } else { Get-Location }

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


bazel shutdown
bazel fetch @nodejs_dev_env//...
bazel build `
  `-`-experimental_execution_log_file ${ARTIFACT_DIRS}/build_execution_windows.log `
  //release:sdk-release-tarball `
  //ledger-test-tool/tool:ledger-api-test-tool_distribute.jar `
  //daml-assistant:daml

git clean -fxd -e 'daml-*.tgz' compatibility/head_sdk

cp -Force bazel-bin\release\sdk-release-tarball-ce.tar.gz compatibility/head_sdk
cp -Force bazel-bin\ledger-test-tool\tool\ledger-api-test-tool_distribute.jar compatibility/head_sdk
cp -Force bazel-bin\daml-assistant\daml.exe compatibility/head_sdk
cp -Force templates\create-daml-app-test-resources\messaging.patch compatibility/head_sdk
