load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")

haskell_cabal_library(
    name = "Cabal",
    srcs = glob(["Cabal/**"]),
    verbose = False,
    version = "3.8.1.0",
    visibility = ["//visibility:public"],
)