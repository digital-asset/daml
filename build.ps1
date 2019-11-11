Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

.\dev-env\windows\bin\dadew.ps1 install
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable

if (!(Test-Path .\.bazelrc.local)) {
   Set-Content -Path .\.bazelrc.local -Value 'build --config windows'
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

# ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
# which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
# which is a workaround for this problem.
bazel shutdown

# Manually fetch a Windows dev-env tool to avoid the following error:
#   ERROR loading ~/.scoop: The process cannot access the file 'C:\Users\VssAdministrator\.scoop' because it is being used by another process.
bazel fetch @tar_dev_env//...
bazel fetch @gzip_dev_env//...
bazel fetch @mvn_dev_env//...
bazel fetch @zip_dev_env//...
bazel fetch @jq_dev_env//...
bazel fetch @javadoc_dev_env//...
bazel fetch @makensis_dev_env//...
bazel fetch @nodejs_dev_env//...
bazel fetch @postgresql_dev_env//...

bazel build `-`-experimental_execution_log_file ${ARTIFACT_DIRS}/build_execution_windows.log //...

bazel shutdown

bazel test `-`-experimental_execution_log_file ${ARTIFACT_DIRS}/test_execution_windows.log //...
