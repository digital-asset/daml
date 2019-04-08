load("@build_bazel_rules_typescript//:defs.bzl", "ts_config")

ts_config(
    name = "tsconfig",
    src = "tsconfig.json",
    deps = ["//:tsconfig.json"],
)

filegroup(
    name = "README",
    srcs = ["README.md"],
    visibility = ['//language-support/js:__subpackages__'],
)

filegroup(
    name = "src",
    srcs = glob(["src/**/*.ts"]),
    visibility = ['//language-support/js:__subpackages__'],
)

filegroup(
    name = "tests",
    srcs = glob(["tests/**/*"])
)

# ts_library does not accept arbitrary inputs, so we use a genrule
genrule(
    name = "daml-ledger",
    srcs = [
        ":src",
        "//language-support/js/daml-grpc",
        "package.json",
        ":tsconfig",
        "@npm//@types/google-protobuf",
        "@npm//@types/node",
        "@npm//google-protobuf",
        "@npm//grpc",
    ],
    outs = ["daml-ledger.tgz"],
    cmd = """
      set -e
      ln -s external/npm/node_modules
      mkdir -p node_modules/daml-grpc
      tar xzf $(location //language-support/js/daml-grpc) --strip-components=1 -C node_modules/daml-grpc
      $(execpath //language-support/js:tsc) --build $(location tsconfig)
      mkdir -p package
      cp -R language-support/js/daml-ledger/dist package
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
        "//language-support/js/daml-grpc",
        ":src",
        ":tests",
        "@nodejs//:node",
        "@npm//@types/chai",
        "@npm//@types/google-protobuf",
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//@types/sinon",
        "@npm//chai",
        "@npm//google-protobuf",
        "@npm//grpc",
        "@npm//jsverify",
        "@npm//mocha",
        "@npm//sinon",
        "@npm//ts-node",
        "@npm//typescript",
    ],
)
