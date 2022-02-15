load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary", "da_haskell_library")

da_haskell_library(
    name = "with-oracle",
    srcs = ["lib/WithOracle.hs"],
    data = [],
    hackage_deps = [
        "base",
        "directory",
        "extra",
        "filepath",
        "network",
        "process",
        "safe-exceptions",
        "text",
        "uuid",
    ],
    visibility = ["//visibility:public"],
)

da_haskell_binary(
    name = "with-oracle-exe",
    srcs = ["exe/Main.hs"],
    hackage_deps = [
        "base",
        "process",
        "text",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":with-oracle",
    ],
)
