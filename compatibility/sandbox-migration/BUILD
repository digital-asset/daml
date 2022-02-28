load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary", "da_haskell_library")
load("@daml//bazel_tools:scala.bzl", "da_scala_binary")
load(":util.bzl", "migration_test")

genrule(
    name = "migration-model",
    srcs = glob(["daml/*.daml"]),
    outs = ["migration-model.dar"],
    cmd = """
TMP=$$(mktemp -d)
trap "rm -rf $$TMP" EXIT
LOCS=($(locations @daml-sdk-1.6.1//:daml))
DAML=$${LOCS[0]}

cat > $$TMP/daml.yaml <<EOF
sdk-version: 1.6.1
source: .
name: migration-model
version: 1.6.1
dependencies: [daml-stdlib, daml-prim]
EOF

cp $(location :daml/Divulgence.daml) $$TMP/Divulgence.daml
cp $(location :daml/KeyTransfer.daml) $$TMP/KeyTransfer.daml
cp $(location :daml/ProposeAccept.daml) $$TMP/ProposeAccept.daml
$$DAML build --project-root $$TMP -o $$PWD/$(location :migration-model.dar)
""",
    # We run the migration tests from 1.6 upwards so we build the DAR using that version.
    tools = ["@daml-sdk-1.6.1//:daml"],
    visibility = ["//visibility:public"],
)

da_scala_binary(
    name = "migration-step",
    srcs = glob(["src/com/daml/**/*.scala"]),
    main_class = "com.daml.MigrationStep",
    resources = ["src/resources/logback.xml"],
    scala_deps = [
        "@maven//:com_daml_daml_lf_archive_reader",
        "@maven//:com_daml_daml_lf_data",
        "@maven//:com_daml_daml_lf_transaction",
        "@maven//:com_daml_ledger_api_client",
        "@maven//:com_daml_ledger_api_common",
        "@maven//:com_daml_ledger_api_domain",
        "@maven//:com_daml_ledger_api_scalapb",
        "@maven//:com_daml_lf_value_json",
        "@maven//:com_daml_rs_grpc_akka",
        "@maven//:com_github_scopt_scopt",
        "@maven//:com_thesamet_scalapb_lenses",
        "@maven//:com_thesamet_scalapb_scalapb_runtime",
        "@maven//:com_typesafe_akka_akka_actor",
        "@maven//:com_typesafe_akka_akka_stream",
        "@maven//:io_spray_spray_json",
        "@maven//:org_scalaz_scalaz_core",
    ],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    deps = [
        "@maven//:com_daml_rs_grpc_bridge",
        "@maven//:io_grpc_grpc_api",
    ],
)

da_haskell_binary(
    name = "sandbox-migration-runner",
    srcs = glob(["runner/**/*.hs"]),
    data = [
        ":migration-step",
        "@daml-sdk-0.0.0//:daml",
    ],
    hackage_deps = [
        "aeson",
        "base",
        "extra",
        "filepath",
        "lens",
        "lens-aeson",
        "mtl",
        "optparse-applicative",
        "process",
        "semver",
        "text",
    ],
    main_function = "Migration.Runner.main",
    visibility = ["//visibility:public"],
    deps = [
        "//bazel_tools/client_server/with-oracle",
        "//bazel_tools/client_server/with-postgres",
        "//bazel_tools/daml_ledger:sandbox-helper",
        "@rules_haskell//tools/runfiles",
    ],
)

exports_files(["test.sh"])
