load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary", "da_haskell_library")
load(":util.bzl", "migration_test")

genrule(
    name = "migration-model",
    srcs = ["daml/Model.daml"],
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

cp $(location :daml/Model.daml) $$TMP/Model.daml
$$DAML build --project-root $$TMP -o $$PWD/$(location :migration-model.dar)
""",
    tools = ["@daml-sdk-1.0.0//:daml"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "migration-script",
    srcs = [
        "daml/Script.daml",
        ":migration-model.dar",
    ],
    outs = ["migration-script.dar"],
    cmd = """
set -eou pipefail
TMP=$$(mktemp -d)
trap "rm -rf $$TMP" EXIT
LOCS=($(locations @daml-sdk-1.0.0//:daml))
DAML=$${LOCS[0]}

cat > $$TMP/daml.yaml <<EOF
sdk-version: 1.0.0
source: .
name: migration-script
version: 1.0.0
dependencies: [daml-stdlib, daml-prim, daml-script, migration-model.dar]
EOF

cp -L $(location :migration-model.dar) $$TMP
cp -L $(location :daml/Script.daml) $$TMP/Script.daml
$$DAML build --project-root $$TMP -o $$PWD/$(location :migration-script.dar)
""",
    tools = ["@daml-sdk-1.0.0//:daml"],
    visibility = ["//visibility:public"],
)

da_haskell_binary(
    name = "sandbox-migration-runner",
    srcs = ["SandboxMigrationRunner.hs"],
    hackage_deps = [
        "aeson",
        "base",
        "extra",
        "filepath",
        "optparse-applicative",
        "process",
        "text",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//bazel_tools/client_server/with-postgres",
        "//bazel_tools/daml_ledger:sandbox-helper",
    ],
)

exports_files(["test.sh"])
