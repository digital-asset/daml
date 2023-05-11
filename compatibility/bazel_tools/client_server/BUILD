load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary")

da_haskell_binary(
    name = "runner",
    srcs = ["Main.hs"],
    hackage_deps = [
        "base",
        "http-client",
        "http-conduit",
        "monad-loops",
        "network",
        "process",
        "safe-exceptions",
        "split",
    ],
    visibility = ["//visibility:public"],
)
