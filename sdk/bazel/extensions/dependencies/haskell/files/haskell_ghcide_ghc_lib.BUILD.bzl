load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@rules_haskell//haskell:defs.bzl", "haskell_library")
load("@stackage//:packages.bzl", "packages")

haskell_cabal_library(
    name = "ghcide",
    version = packages["ghcide"].version,
    srcs = glob(["**"]),
    haddock = False,
    flags = packages["ghcide"].flags,
    deps = packages["ghcide"].deps,
    visibility = ["//visibility:public"],
)
haskell_library(
    name = "testing",
    srcs = glob(["test/src/**/*.hs"]),
    src_strip_prefix = "test/src",
    deps = [
        "@stackage//:aeson",
        "@stackage//:base",
        "@stackage//:extra",
        "@stackage//:containers",
        "@stackage//:lsp-types",
        "@stackage//:lens",
        "@stackage//:lsp-test",
        "@stackage//:parser-combinators",
        "@stackage//:tasty-hunit",
        "@stackage//:text",
    ],
    ghcopts = [
        "-XBangPatterns",
        "-XDeriveFunctor",
        "-XDeriveGeneric",
        "-XGeneralizedNewtypeDeriving",
        "-XLambdaCase",
        "-XNamedFieldPuns",
        "-XOverloadedStrings",
        "-XRecordWildCards",
        "-XScopedTypeVariables",
        "-XStandaloneDeriving",
        "-XTupleSections",
        "-XTypeApplications",
        "-XViewPatterns",
    ],
    visibility = ["//visibility:public"],
)
