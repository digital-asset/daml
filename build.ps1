param ([String]$mode = 'full')

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
        //:git-revision `
        //compiler/daml-lf-ast/... `
        //compiler/haskell-ide-core/... `
        //daml-lf/interface/... `
        //language-support/java/bindings/... `
        //navigator/backend/... `
        //navigator/frontend/...

    bazel shutdown

    bazel test `
        //daml-lf/interface/... `
        //language-support/java/bindings/... `
        //navigator/backend/...
}

function build-full() {
    # FIXME: Until all bazel issues on Windows are resolved we will be testing only specific bazel targets
    bazel build `
        //:git-revision `
        @com_github_grpc_grpc//:grpc `
        //nix/third-party/gRPC-haskell:grpc-haskell `
        //daml-assistant:daml `
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
        //daml-foundations/daml-tools/da-hs-damlc-app:damlc-dist `
        //language-support/java/testkit:testkit `
        //language-support/java/bindings/... `
        //language-support/java/bindings-rxjava/... `
        //ledger/backend-api/... `
        //ledger/ledger-api-client/... `
        //ledger/ledger-api-common/... `
        //ledger/ledger-api-domain/... `
        //ledger/ledger-api-server-example `
        //ledger-api/rs-grpc-akka/... `
        //navigator/backend/... `
        //navigator/frontend/... `
        //pipeline/... `
        //scala-protoc-plugins/...

    # ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
    # which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
    # which is a workaround for this problem.
    bazel shutdown

    bazel run `
        //daml-foundations/daml-tools/da-hs-damlc-app `-`- `-h

    # ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
    # which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
    # which is a workaround for this problem.
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
        //ledger-api/rs-grpc-akka/... `
        //navigator/backend/... `
        //pipeline/...
}

# FIXME:
# @haskell_c2hs//... `
#ERROR: C:/users/vssadministrator/_bazel_vssadministrator/w3d6ug6o/external/haskell_c2hs/BUILD.bazel:16:3: unterminated string literal at eol
#ERROR: C:/users/vssadministrator/_bazel_vssadministrator/w3d6ug6o/external/haskell_c2hs/BUILD.bazel:17:1: unterminated string literal at eol
#ERROR: C:/users/vssadministrator/_bazel_vssadministrator/w3d6ug6o/external/haskell_c2hs/BUILD.bazel:17:1: Implicit string concatenation is forbidden, use the + operator
#ERROR: C:/users/vssadministrator/_bazel_vssadministrator/w3d6ug6o/external/haskell_c2hs/BUILD.bazel:17:1: syntax error at '",
#"': expected ,

Write-Output "Running in $mode mode"
if ($mode -eq "partial") {
    build-partial
} else {
    build-full
}
