load(
    "@rules_haskell//haskell:defs.bzl",
    "haskell_binary",
)

haskell_binary(
    name = "runner",
    srcs = ["Main.hs"],
    deps = [
        "@stackage//:base",
        "@stackage//:monad-loops",
        "@stackage//:network",
        "@stackage//:process",
        "@stackage//:safe-exceptions",
        "@stackage//:split",
    ],
    visibility = ["//visibility:public"],
)
