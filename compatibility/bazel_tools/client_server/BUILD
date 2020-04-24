load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary")

da_haskell_binary(
    name = "runner",
    srcs = ["Main.hs"],
    hackage_deps = [
        "base",
        "monad-loops",
        "network",
        "process",
        "safe-exceptions",
        "split",
    ],
    visibility = ["//visibility:public"],
)

da_haskell_binary(
    name = "with-postgres",
    srcs = ["WithPostgres.hs"],
    data = [
        "@postgresql_nix//:all",
    ],
    hackage_deps = [
        "base",
        "directory",
        "extra",
        "filepath",
        "process",
        "text",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@rules_haskell//tools/runfiles",
    ],
)
