param ([String]$mode = 'local')

Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

.\dev-env\windows\bin\dadew.ps1 install
.\dev-env\windows\bin\dadew.ps1 sync
.\dev-env\windows\bin\dadew.ps1 enable

function bazel() {
    Write-Output ">> bazel $args"
    $global:lastexitcode = 0
    $backupErrorActionPreference = $script:ErrorActionPreference
    $script:ErrorActionPreference = "Continue"
    & bazel.exe --bazelrc=.\nix\bazelrc @args 2>&1 | %{ "$_" }
    $script:ErrorActionPreference = $backupErrorActionPreference
    if ($global:lastexitcode -ne 0 -And $args[0] -ne "shutdown") {
        Write-Output "<< bazel $args (failed, exit code: $global:lastexitcode)"
        throw ("Bazel returned non-zero exit code: $global:lastexitcode")
    }
    Write-Output "<< bazel $args (ok)"
}

function build-partial() {
    bazel build `
        //compiler/daml-lf-ast/... `
        //daml-foundations/daml-tools/daml-extension:daml_extension_lib `
        //daml-foundations/daml-tools/language-server-tests:lib-js `
        //daml-lf/interface/... `
        //daml-foundations/daml-tools/docs/... `
        //language-support/java/bindings/...

    bazel shutdown

    bazel test `
        //daml-lf/interface/... `
        //language-support/java/bindings/...
}

function build-full() {
    # FIXME: Until all bazel issues on Windows are resolved we will be testing only specific bazel targets
    bazel build `
        @com_github_grpc_grpc//:grpc `
        //nix/third-party/gRPC-haskell/core:fat_cbits `
        @haskell_c2hs//... `
        //daml-foundations/daml-tools/daml-extension:daml_extension_lib `
        //daml-foundations/daml-tools/language-server-tests:lib-js `
        //daml-lf/archive:daml_lf_archive_scala `
        //daml-lf/archive:daml_lf_archive_protos_zip `
        //daml-lf/archive:daml_lf_archive_protos_tarball `
        //compiler/haskell-ide-core/... `
        //compiler/daml-lf-ast/... `
        //daml-lf/data/... `
        //daml-lf/engine:engine `
        //daml-lf/interface/... `
        //daml-lf/interpreter/... `
        //daml-lf/lfpackage/... `
        //daml-lf/parser/... `
        //daml-lf/repl/... `
        //daml-lf/scenario-interpreter/... `
        //daml-lf/transaction-scalacheck/... `
        //daml-lf/validation/... `
        //daml-foundations/daml-tools/docs/... `
        //language-support/java/testkit:testkit `
        //language-support/java/bindings/... `
        //language-support/java/bindings-rxjava/... `
        //ledger/backend-api/... `
        //ledger/ledger-api-client/... `
        //ledger/ledger-api-common/... `
        //ledger/ledger-api-domain/... `
        //ledger/ledger-api-server-example `
        //ledger-api/rs-grpc-akka/... `
        //pipeline/samples/bazel/java/... `
        //pipeline/samples/bazel/haskell/...

    # ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
    # which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
    # which is a workaround for this problem.
    bazel shutdown

    bazel test `
        //daml-lf/data/... `
        //daml-lf/interface/... `
        //daml-lf/lfpackage/... `
        //daml-lf/parser/... `
        //daml-lf/validation/... `
        //language-support/java/bindings/... `
        //language-support/java/bindings-rxjava/... `
        //ledger/ledger-api-client/... `
        //ledger/ledger-api-common/... `
        //ledger-api/rs-grpc-akka/... `
        //pipeline/samples/bazel/java/... `
        //pipeline/samples/bazel/haskell/...
}

# FIXME: one of tests fails
#bazel test //daml-lf/interpreter/...

Write-Output "Running in $mode mode"
if ($mode -eq "ci") {
    build-partial
} else {
    build-full
}
