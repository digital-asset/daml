load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_binary", "haskell_cabal_library")

haskell_cabal_library(
    name = "ghc-lib-gen-lib",
    package_name = "ghc-lib-gen",
    version = "0.1.0.0",
    haddock = False,
    srcs = glob(["**"]),
    deps = [
        "@stackage//:base",
        "@stackage//:process",
        "@stackage//:filepath",
        "@stackage//:containers",
        "@stackage//:directory",
        "@stackage//:optparse-applicative",
        "@stackage//:bytestring",
        "@stackage//:yaml",
        "@stackage//:aeson",
        "@stackage//:text",
        "@stackage//:unordered-containers",
        "@stackage//:extra",
    ],
)

haskell_cabal_binary(
    name = "ghc-lib-gen",
    srcs = glob(["**"]),
    deps = [
        ":ghc-lib-gen-lib",
        "@stackage//:base",
        "@stackage//:containers",
        "@stackage//:directory",
        "@stackage//:extra",
        "@stackage//:filepath",
        "@stackage//:optparse-applicative",
        "@stackage//:process",
    ],
    visibility = ["//visibility:public"],
)