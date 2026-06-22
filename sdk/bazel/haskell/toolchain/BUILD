load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel/native:configure_make.bzl", "configure_make")
load("//bazel/versions:ghc.version.bzl", "GHC_VERSION")
load(":defs.bzl", "ghc_smoke", "ghc_toolchain")
load(":ghc_bindist_install.bzl", "ghc_bindist_install")

ghc_bindist_install(
    name = "ghc_install",
    srcs = "@ghc_bindist//:bindist_srcs",
    configure = "@ghc_bindist//:bindist_unpacked/configure",
    gmp = "@gmp//:libs",
    make = "@make//:make",
    tinfo = ":libtinfo.so.5",
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

configure_make(
    name = "tinfo",
    srcs = "@ncurses//:srcs",
    configure = "@ncurses//:configure",
    make = "@make//:make",
    configure_flags = [
        "--build=x86_64-unknown-linux-gnu",
        "--host=x86_64-pc-linux-gnu",
        "--with-shared",
        "--with-termlib",
        "--with-versioned-syms",
        "--without-debug",
        "--without-ada",
        "--without-cxx-binding",
        "--without-manpages",
        "--without-tests",
    ],
    install_libdir = "lib",
    # ncurses 5.9 predates C99 implicit-decl errors; clang 22 makes them fatal.
    extra_cflags = ["-Wno-implicit-function-declaration"],
    # 5.9 has no --with-versioned-syms; this script stamps the exported symbols
    # with the NCURSES_TINFO_5.0.19991023 node the deb9 GHC binaries require.
    version_script = "files/libtinfo5.map",
    outs = [
        "libtinfo.so.5",
        "libtinfo.so",
    ],
)

cc_library(
    name = "tinfo_cc_lib",
    srcs = [":libtinfo.so"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "tinfo_libs",
    srcs = [
        ":libtinfo.so.5",
        ":libtinfo.so",
    ],
    visibility = ["//visibility:public"],
)
