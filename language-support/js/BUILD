load("@build_bazel_rules_typescript//:defs.bzl", "ts_config", "ts_library")
load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary", "nodejs_test", "npm_package")

nodejs_binary(
    name = "grpc_tools_node_protoc",
    data = [
        "@npm//grpc-tools",
    ],
    entry_point = "grpc-tools/bin/protoc.js",
    visibility = ["//language-support/js:__subpackages__"]
)

nodejs_binary(
    name = "tsc",
    data = [
        "@npm//typescript",
    ],
    entry_point = "typescript/bin/tsc",
    visibility = ["//language-support/js:__subpackages__"],
)
