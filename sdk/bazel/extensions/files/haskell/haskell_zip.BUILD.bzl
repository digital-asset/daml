load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")

haskell_cabal_library(
    name = "zip",
    version = packages["zip"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["zip"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
    flags = packages["zip"].flags,
)
