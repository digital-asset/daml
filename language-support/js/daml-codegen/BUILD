load("@build_bazel_rules_typescript//:defs.bzl", "ts_config")

ts_config(
    name = "tsconfig",
    src = "tsconfig.json",
    deps = ["//:tsconfig.json"],
)

filegroup(
    name = "src",
    srcs = glob(["*.ts"]),
)

# ts_library does not accept arbitrary inputs, so we use a genrule
genrule(
    name = "daml-codegen",
    srcs = [
        ":src",
        "//language-support/js/daml-grpc",
        "//language-support/js/daml-ledger",
        "package.json",
        ":tsconfig",
        "@npm//@types/google-protobuf",
        "@npm//@types/node",
        "@npm//google-protobuf",
        "@npm//grpc",
        "@npm//typescript",
    ],
    outs = ["daml-codegen.tgz"],
    cmd = """
      set -e
      ln -s external/npm/node_modules
      mkdir -p node_modules/daml-grpc
      tar xzf $(location //language-support/js/daml-grpc) --strip-components=1 -C node_modules/daml-grpc
      mkdir -p node_modules/daml-ledger
      tar xzf $(location //language-support/js/daml-ledger) --strip-components=1 -C node_modules/daml-ledger
      $(execpath //language-support/js:tsc) --build $(location tsconfig)
      mkdir -p package
      cp -R language-support/js/daml-codegen/dist package
      cp $(location package.json) package
      tar czf $@ package
    """,
    tools = [
        "//language-support/js:tsc",
    ],
)
