Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

# See https://github.com/lukesampson/scoop/issues/3859
Set-Strictmode -Off
.\dev-env\windows\bin\dadew.ps1 install
Set-StrictMode -Version latest
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable

if (!(Test-Path .\.bazelrc.local)) {
   Set-Content -Path .\.bazelrc.local -Value 'build --config windows'
}

$ARTIFACT_DIRS = if ("$env:BUILD_ARTIFACTSTAGINGDIRECTORY") { $env:BUILD_ARTIFACTSTAGINGDIRECTORY } else { Get-Location }

if (!(Test-Path ${ARTIFACT_DIRS}/logs)) {
    mkdir -p ${ARTIFACT_DIRS}/logs
} elseif (Test-Path ${ARTIFACT_DIRS}/logs -PathType Leaf) {
    throw ("Cannot create directory '${ARTIFACT_DIRS}/logs'. Conflicting file.")
}

# If a previous build was forcefully terminated, then stack's lock file might
# not have been cleaned up properly leading to errors of the form
#
#   user error (hTryLock: lock already exists: C:\Users\u\AppData\Roaming\stack\pantry\hackage\hackage-security-lock)
#
# The package cache might be corrupted and just removing the lock might lead to
# errors as below, so we just nuke the entire stack cache.
#
#   Failed populating package index cache
#   IncompletePayload 56726464 844
#
if (Test-Path -Path $env:appdata\stack\pantry\hackage\hackage-security-lock) {
    Write-Output ">> Nuking stack directory"
    Remove-Item -ErrorAction Continue -Force -Recurse -Path $env:appdata\stack
}

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

bazel test //compiler/lsp-tests --runs_per_test=100
