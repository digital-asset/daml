load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary", "da_haskell_library")
load("@daml//bazel_tools:scala.bzl", "da_scala_binary")
load(":util.bzl", "migration_test")

genrule(
    name = "migration-model",
    srcs = ["daml/ProposeAccept.daml"],
    outs = ["migration-model.dar"],
    cmd = """
TMP=$$(mktemp -d)
trap "rm -rf $$TMP" EXIT
LOCS=($(locations @daml-sdk-1.0.0//:daml))
DAML=$${LOCS[0]}

cat > $$TMP/daml.yaml <<EOF
sdk-version: 1.0.0
source: .
name: migration-model
version: 1.0.0
dependencies: [daml-stdlib, daml-prim]
EOF

cp $(location :daml/ProposeAccept.daml) $$TMP/ProposeAccept.daml
$$DAML build --project-root $$TMP -o $$PWD/$(location :migration-model.dar)
""",
    tools = ["@daml-sdk-1.0.0//:daml"],
    visibility = ["//visibility:public"],
)

da_scala_binary(
    name = "migration-step",
    srcs = glob(["src/com/daml/**/*.scala"]),
    main_class = "com.daml.MigrationStep",
    resources = ["src/resources/logback.xml"],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    deps = [
        "@maven//:com_daml_daml_lf_archive_reader_2_12",
        "@maven//:com_daml_daml_lf_data_2_12",
        "@maven//:com_daml_daml_lf_dev_archive_java_proto",
        "@maven//:com_daml_daml_lf_transaction_2_12",
        "@maven//:com_daml_ledger_api_client_2_12",
        "@maven//:com_daml_ledger_api_common_2_12",
        "@maven//:com_daml_ledger_api_domain_2_12",
        "@maven//:com_daml_ledger_api_scalapb_2_12",
        "@maven//:com_daml_lf_value_json_2_12",
        "@maven//:com_daml_rs_grpc_akka_2_12",
        "@maven//:com_daml_rs_grpc_bridge",
        "@maven//:com_github_scopt_scopt_2_12",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_thesamet_scalapb_lenses_2_12",
        "@maven//:com_thesamet_scalapb_scalapb_runtime_2_12",
        "@maven//:com_typesafe_akka_akka_actor_2_12",
        "@maven//:com_typesafe_akka_akka_stream_2_12",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_spray_spray_json_2_12",
        "@maven//:org_scalaz_scalaz_core_2_12",
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
        "text",
    ],
    main_function = "Migration.Runner.main",
    visibility = ["//visibility:public"],
    deps = [
        "//bazel_tools/client_server/with-postgres",
        "//bazel_tools/daml_ledger:sandbox-helper",
        "@rules_haskell//tools/runfiles",
    ],
)

exports_files(["test.sh"])
