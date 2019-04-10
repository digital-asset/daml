load("@build_bazel_rules_typescript//:defs.bzl", "ts_config")
load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")
load("//bazel_tools:proto.bzl", "proto_gen")

proto_gen(
    name = "ledger-api-nodejs",
    srcs = ["//ledger-api/grpc-definitions:protos"],
    deps = [
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:descriptor_proto",
        "@com_github_googleapis_googleapis//google/rpc:status_proto",
    ],
    plugin_name = "js",
    plugin_options = ["import_style=commonjs", "binary"],
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_js",
)

proto_gen(
    name = "ledger-api-nodejs-grpc",
    srcs = ["//ledger-api/grpc-definitions:protos"],
    deps = [
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:descriptor_proto",
        "@com_github_googleapis_googleapis//google/rpc:status_proto",
    ],
    plugin_name = "grpc",
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_js",
)

proto_gen(
    name = "ledger-api-d.ts",
    srcs = ["//ledger-api/grpc-definitions:protos"],
    deps = [
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:descriptor_proto",
        "@com_github_googleapis_googleapis//google/rpc:status_proto",
    ],
    plugin_name = "ts",
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_ts",
)

proto_gen(
    name = "daml_lf_proto-nodejs",
    srcs = ["//daml-lf/archive:daml_lf_proto"],
    plugin_name = "js",
    plugin_options = ["import_style=commonjs", "binary"],
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_js",
)

proto_gen(
    name = "daml_lf_proto-d.ts",
    srcs = ["//daml-lf/archive:daml_lf_proto"],
    plugin_name = "ts",
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_ts",
)

proto_gen(
    name = "com_google_protobuf-nodejs",
    srcs = [
        "@com_github_googleapis_googleapis//google/rpc:status_proto",
    ],
    deps = [
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
    plugin_name = "js",
    plugin_options = ["import_style=commonjs", "binary"],
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_js",
)

proto_gen(
    name = "com_google_protobuf-d.ts",
    srcs = [
        "@com_github_googleapis_googleapis//google/rpc:status_proto",
    ],
    deps = [
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
    plugin_name = "ts",
    plugin_exec = "//language-support/js:grpc_tools_node_protoc_ts",
)

ts_config(
    name = "tsconfig",
    src = "tsconfig.json",
    deps = ["//:tsconfig.json"],
)

# ts_library does not accept arbitrary inputs, so we use a genrule
genrule(
    name = "daml-grpc",
    srcs = [
        "index.ts",
        "lf/index.ts",
        "testing/index.ts",
        ":ledger-api-nodejs",
        ":ledger-api-nodejs-grpc",
        ":ledger-api-d.ts",
        ":daml_lf_proto-nodejs",
        ":daml_lf_proto-d.ts",
        ":com_google_protobuf-nodejs",
        ":com_google_protobuf-d.ts",
        "package.json",
        ":tsconfig",
        "@npm//@types/google-protobuf",
        "@npm//@types/node",
        "@npm//google-protobuf",
        "@npm//grpc",
    ],
    outs = ["daml-grpc.tgz"],
    cmd = """
      set -e
      ln -s external/npm/node_modules
      unzip -q $(location ledger-api-nodejs) -d language-support/js/daml-grpc/
      unzip -q $(location ledger-api-nodejs-grpc) -d language-support/js/daml-grpc/
      unzip -q $(location ledger-api-d.ts) -d language-support/js/daml-grpc/
      unzip -q $(location daml_lf_proto-nodejs) -d language-support/js/daml-grpc/
      unzip -q $(location daml_lf_proto-d.ts) -d language-support/js/daml-grpc/
      unzip -q $(location com_google_protobuf-nodejs) -d language-support/js/daml-grpc/
      unzip -q $(location com_google_protobuf-d.ts) -d language-support/js/daml-grpc/
      $(execpath //language-support/js:tsc) --build $(location tsconfig)
      cp -R language-support/js/daml-grpc/dist package
      cp -RL language-support/js/daml-grpc/com package
      cp -RL language-support/js/daml-grpc/da package
      cp -RL language-support/js/daml-grpc/google package
      cp -RL language-support/js/daml-grpc/grpc package
      cp $(location package.json) package
      tar czf $@ package
    """,
    tools = [
        "//language-support/js:tsc",
    ],
    visibility = ["//:__subpackages__"]
)

# nodejs_test does not support mocha, so we use sh_test
sh_test(
    name = "test",
    srcs = ["test.sh"],
    data = [
        ":daml-grpc",
        "test.ts",
        "@nodejs//:node",
        "@npm//@types/chai",
        "@npm//@types/google-protobuf",
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//google-protobuf",
        "@npm//grpc",
        "@npm//mocha",
        "@npm//ts-node",
    ],
)
