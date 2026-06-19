load("//bazel/versions:ghc.version.bzl", "GHC_VERSION")
load(":defs.bzl", "ghc_smoke", "ghc_toolchain")
load(":ghc_bindist_install.bzl", "ghc_bindist_install")

ghc_bindist_install(
    name = "ghc_install",
    srcs = "@ghc_bindist//:bindist_srcs",
    configure = "@ghc_bindist//:bindist_unpacked/configure",
    gmp = "@gmp//:libs",
    make = "@make//:make",
    visibility = ["//visibility:public"],
)

toolchain_type(name = "ghc_toolchain_type")

ghc_toolchain(
    name = "ghc_impl",
    install = ":ghc_install",
    version = GHC_VERSION,
    visibility = ["//visibility:public"],
)

toolchain(
    name = "ghc_linux_amd64",
    exec_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    toolchain = ":ghc_impl",
    toolchain_type = ":ghc_toolchain_type",
)

ghc_smoke(name = "smoke")
