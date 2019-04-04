Set-StrictMode -Version latest
$ErrorActionPreference = 'Stop'

function bazel() {
    Write-Output ">> bazel $args"
    $global:lastexitcode = 0
    . bazel.exe --bazelrc=.\nix\bazelrc --host_jvm_args=-Djavax.net.ssl.trustStore="$(dadew where)\current\apps\da-truststore\cacerts" @args
    if ($global:lastexitcode -ne 0) {
        throw ("Bazel returned non-zero exit code: $global:lastexitcode")
    }
}

$env:BAZEL_SH = [Environment]::GetEnvironmentVariable("BAZEL_SH", [System.EnvironmentVariableTarget]::User)

# FIXME: Until all bazel issues on Windows are resolved we will be testing only specific bazel targets

# general workspace test
bazel test //pipeline/samples/bazel/java/...

# zipper on Windows
# NOTE(FM): before we were just pulling the external sandbox, now  we're building it,
# and it does not work.
# bazel build //ledger/sandbox:sandbox

# basic test for the haskell infrastructure
bazel build //pipeline/samples/bazel/haskell/...
bazel build //compiler/haskell-ide-core/...

# node / npm / yarn test
bazel build //daml-foundations/daml-tools/daml-extension:daml_extension_lib
bazel build //daml-foundations/daml-tools/language-server-tests:lib-js

# ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
# which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
# which is a workaround for this problem.
# bazel shutdown

##################################################################
## ledger
# bazel build //ledger/backend-api/...
# bazel shutdown

# bazel build //ledger/ledger-api-client/...
# bazel shutdown

# bazel test //ledger/ledger-api-client/...
# bazel shutdown

# bazel build //ledger/ledger-api-common/...
# bazel shutdown

# bazel test //ledger/ledger-api-common/...
# bazel shutdown

# bazel build //ledger/ledger-api-domain/...
# bazel shutdown

# bazel build //ledger/ledger-api-server-example
# bazel shutdown

##################################################################
## ledger-api
# bazel build //ledger-api/rs-grpc-akka/...
# bazel shutdown

# bazel test //ledger-api/rs-grpc-akka/...
# bazel shutdown

###################################################################
## daml-lf (some parts of it are still not building correctly
# - falling back to target by target build until all issues are resolved)

# TODO: haskell targets left in //daml-lf/archive
# bazel build //daml-lf/archive:daml_lf_archive_scala
# bazel shutdown
# bazel build //daml-lf/archive:daml_lf_archive_protos_zip
# bazel build //daml-lf/archive:daml_lf_archive_protos_tarball

# bazel build //daml-lf/data/...
# bazel shutdown

# bazel test //daml-lf/data/...
# bazel shutdown

# bazel build //daml-lf/engine:engine
# bazel shutdown

# bazel build //daml-lf/interface/...
# bazel shutdown

# bazel test //daml-lf/interface/...
# bazel shutdown

# bazel build //daml-lf/interpreter/...
# bazel shutdown

# FIXME: one of tests fails
#bazel test //daml-lf/interpreter/...
#bazel shutdown

# bazel build //daml-lf/lfpackage/...
# bazel shutdown

# bazel test //daml-lf/lfpackage/...
# bazel shutdown

# bazel build //daml-lf/parser/...
# bazel shutdown

# bazel test //daml-lf/parser/...
# bazel shutdown

# bazel build //daml-lf/repl/...
# bazel shutdown

#no tests
#bazel test //daml-lf/repl/...
#bazel shutdown

# bazel build //daml-lf/scenario-interpreter/...
# bazel shutdown

#no tests
#bazel test //daml-lf/scenario-interpreter/...
#bazel shutdown

# bazel build //daml-lf/transaction-scalacheck/...
# bazel shutdown

#no tests
#bazel test //daml-lf/transaction-scalacheck/...
#bazel shutdown

# bazel build //daml-lf/validation/...
# bazel shutdown

# bazel test //daml-lf/validation/...
# bazel shutdown
###################################################################

# bazel build //daml-foundations/daml-tools/docs/...

##############################################################
## language-support

# bazel build //language-support/java/testkit:testkit
# bazel shutdown

# bazel build //language-support/java/bindings/...
# bazel shutdown

# bazel test //language-support/java/bindings/...
# bazel shutdown

# bazel build //language-support/java/bindings-rxjava/...
# bazel shutdown

# bazel test //language-support/java/bindings-rxjava/...
# bazel shutdown

###################################################################
