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

haskell_binary(
    name = "with-postgres",
    srcs = ["WithPostgres.hs"],
    data = [
        "@postgresql_nix//:all",
    ],
    deps = [
        "@stackage//:base",
        "@rules_haskell//tools/runfiles",
        "@stackage//:directory",
        "@stackage//:extra",
        "@stackage//:filepath",
        "@stackage//:process",
        "@stackage//:text",
    ],
    visibility = ["//visibility:public"],
)
