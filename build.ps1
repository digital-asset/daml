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

# FIXME: Until all bazel issues on Windows are resolved we will be testing only specific bazel targets
bazel build `-`-experimental_execution_log_file ${ARTIFACT_DIRS}/build_full_execution_windows.log `
    //release:sdk-release-tarball `
    //release/windows-installer:windows-installer `
    //:git-revision `
    @com_github_grpc_grpc//:grpc `
    @haskell_c2hs//... `
    //3rdparty/... `
    //nix/third-party/gRPC-haskell:grpc-haskell `
    //daml-assistant:daml `
    //daml-foundations/daml-tools/... `
    //compiler/... `
    //daml-lf/... `
    //extractor/... `
    //language-support/java/testkit:testkit `
    //language-support/java/bindings/... `
    //language-support/java/bindings-rxjava/... `
    //ledger/... `
    //ledger-api/... `
    //navigator/backend/... `
    //navigator/frontend/... `
    //scala-protoc-plugins/...

bazel shutdown

bazel run `
    //daml-foundations/daml-tools/da-hs-damlc-app `-`- `-h

bazel shutdown

bazel test `
    //daml-lf/data/... `
    //daml-lf/interface/... `
    //daml-lf/interpreter/... `
    //daml-lf/lfpackage/... `
    //daml-lf/parser/... `
    //daml-lf/validation/... `
    //language-support/java/bindings/... `
    //language-support/java/bindings-rxjava/... `
    //ledger/ledger-api-client/... `
    //ledger/ledger-api-common/... `
    //ledger-api/... `
    //navigator/backend/...
