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

# ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
# which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
# which is a workaround for this problem.
bazel shutdown

# Prefetch nodejs_dev_env to avoid permission denied errors on external/nodejs_dev_env/nodejs_dev_env/node.exe
# It isn’t clear where exactly those errors are coming from.
bazel fetch @nodejs_dev_env//...

bazel build //... `
  `-`-profile build-profile.json `
  `-`-experimental_profile_include_target_label `
  `-`-build_event_json_file build-events.json `
  `-`-build_event_publish_all_actions `
  `-`-build_tag_filters=-canton-ee

bazel shutdown

function Has-Run-All-Tests-Trailer {
  if (2 -eq ((git show -s --format=%p HEAD | Measure-Object -Word).Words)) {
    $ref = "HEAD^2"
  } else {
    $ref = "HEAD"
  }
  $commit = git rev-parse $ref
  $run_all_tests = git log -n1 --format="%(trailers:key=run-all-tests,valueonly)" $commit
  $run_all_tests -eq "true"
}

# Generate mapping from shortened scala-test names on Windows to long names on Linux and MacOS.
./ci/remap-scala-test-short-names.ps1 `
  | Out-File -Encoding UTF8 -NoNewline scala-test-suite-name-map.json

$ALL_TESTS_FILTER = "-pr-only"
$FEWER_TESTS_FILTER = "-main-only"

$tag_filter = "-dev-canton-test,-canton-ee"
switch ($env:TEST_MODE) {
  'main' {
    $tag_filter = "$tag_filter,$ALL_TESTS_FILTER"
  }
  'pr' {
    if (Has-Run-All-Tests-Trailer) {
      Write-Output "ignoring 'pr' test mode because the commit message features 'run-all-tests: true'"
      $tag_filter = "$tag_filter,$ALL_TESTS_FILTER"
    } else {
      $tag_filter = "$tag_filter,$FEWER_TESTS_FILTER"
    }
  }
  Default {
    Write-Output "<< unknown test mode: $env:TEST_MODE)"
    throw ("Was given an unknown test mode: $env:TEST_MODE")
  }
}

Write-Output "Running bazel test with the following tag filters: $tag_filter"

bazel test //... `
  `-`-build_tag_filters "$tag_filter" `
  `-`-test_tag_filters "$tag_filter" `
  `-`-profile test-profile.json `
  `-`-experimental_profile_include_target_label `
  `-`-build_event_json_file test-events.json `
  `-`-build_event_publish_all_actions `
