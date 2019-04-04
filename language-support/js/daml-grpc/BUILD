load("@build_bazel_rules_typescript//:defs.bzl", "ts_config")
load(":defs.bzl", "nodejs_proto_compile")

nodejs_proto_compile(
    name = "daml-grpc-srcs"
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
        ":daml-grpc-srcs",
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
      cp -R $(@D)/src/* language-support/js/daml-grpc/
      $(execpath //language-support/js:tsc) --build $(location tsconfig)
      mkdir -p package/dist
      cp -R language-support/js/daml-grpc/dist package
      cp -RL $(@D)/src/* package/dist
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
