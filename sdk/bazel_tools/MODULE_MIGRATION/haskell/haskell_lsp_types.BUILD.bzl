load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")

haskell_cabal_library(
    name = "lsp-types",
    version = packages["lsp-types"].version,
    srcs = glob(["**"]),
    deps = packages["lsp-types"].deps,
    haddock = False,
    visibility = ["//visibility:public"],
)
