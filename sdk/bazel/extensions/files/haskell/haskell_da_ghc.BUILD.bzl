load("@os_info//:os_info.bzl", "is_darwin")
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_binary")
load(
    "@//bazel_tools/ghc-lib:version.bzl",
    "GHC_CPP_OPTIONS",
    "GHC_FLAVOR",
    "GHC_LIB_VERSION",
)

filegroup(
    name = "hadrian-srcs",
    srcs = glob(["hadrian/**"]),
    visibility = ["//visibility:public"],
)

haskell_cabal_binary(
    name = "hadrian",
    flags = ["with_bazel"],
    srcs = [":hadrian-srcs"],
    deps = [
        "@stackage//:base",
        "@stackage//:Cabal",
        "@stackage//:containers",
        "@stackage//:directory",
        "@stackage//:extra",
        "@stackage//:mtl",
        "@stackage//:parsec",
        "@stackage//:QuickCheck",
        "@stackage//:shake",
        "@stackage//:transformers",
        "@stackage//:unordered-containers",
    ],
    tools = [
        "@stackage-exe//alex",
        "@stackage-exe//happy",
    ],
    cabalopts = [
        "--ghc-option=-Wno-dodgy-imports",
        "--ghc-option=-Wno-unused-imports",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
