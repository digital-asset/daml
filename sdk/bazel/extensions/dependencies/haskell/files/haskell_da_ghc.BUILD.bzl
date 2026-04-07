load(
    "@//bazel_tools/ghc-lib:version.bzl",
    "GHC_CPP_OPTIONS",
    "GHC_FLAVOR",
    "GHC_LIB_VERSION",
)
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_binary")
load("@//bazel/rules:ghc_lib_sdist.bzl", "ghc_lib_sdist")

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

ghc_lib_sdist(
    name = "ghc-lib",
    ghc_srcs = ":srcs",
    readme = ":README.md",
    ghc_lib_gen = "@ghc-lib-gen//:ghc-lib-gen",
    hadrian = ":hadrian",
    autotools = "@automake//:automake",
    m4 = "@m4//:m4",
    perl = "@rules_perl//:current_toolchain",
    cabal = "@cabal//:cabal",
    component = "",
    version = GHC_LIB_VERSION,
    ghc_flavor = GHC_FLAVOR,
    extra_tools = [
        "@stackage-exe//happy",
        "@stackage-exe//alex",
    ],
    cpp_options = GHC_CPP_OPTIONS,
    visibility = ["//visibility:public"],
)

ghc_lib_sdist(
    name = "ghc-lib-parser",
    ghc_srcs = ":srcs",
    readme = ":README.md",
    ghc_lib_gen = "@ghc-lib-gen//:ghc-lib-gen",
    hadrian = ":hadrian",
    autotools = "@automake//:automake",
    m4 = "@m4//:m4",
    perl = "@rules_perl//:current_toolchain",
    cabal = "@cabal//:cabal",
    component = "-parser",
    version = GHC_LIB_VERSION,
    ghc_flavor = GHC_FLAVOR,
    extra_tools = [
        "@stackage-exe//happy",
        "@stackage-exe//alex",
    ],
    cpp_options = GHC_CPP_OPTIONS,
    visibility = ["//visibility:public"],
)
