load(
    "@//bazel/native:build_make.bzl",
    "build_make",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

build_make(
    name = "make_from_source",
    srcs = ":srcs",
    configure = "configure",
    make_binary = "bin/make",
)

alias(
    name = "make",
    actual = select({
        "@platforms//os:windows": "@msys_make//:make",
        "//conditions:default": ":make_from_source",
    }),
    visibility = ["//visibility:public"],
)
