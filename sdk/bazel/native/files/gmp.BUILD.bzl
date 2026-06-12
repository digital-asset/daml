load(
    "@//bazel/native:configure_make.bzl",
    "configure_make",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

configure_make(
    name = "gmp_build",
    srcs = ":srcs",
    configure = "configure",
    make = "@make//:make",
    m4 = "@m4//:m4",
    configure_flags = [
        "--enable-shared",
        "--disable-static",
    ],
    outs = [
        "lib/libgmp.so",
        "lib/libgmp.so.10",
    ],
)

filegroup(
    name = "libs",
    srcs = [":gmp_build"],
    visibility = ["//visibility:public"],
)

# Wraps the hermetic libgmp.so as a cc_library so it can be passed to
# rules_haskell's stack_snapshot `extra_deps` (which expects CcInfo
# providers). GHC's integer-gmp emits `-lgmp` into every Haskell link
# command; without this, the hermetic sysroot's ld cannot find
# `-lgmp`.
cc_library(
    name = "gmp_cc_lib",
    srcs = ["lib/libgmp.so"],
    visibility = ["//visibility:public"],
)
