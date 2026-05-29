load(
    "@//bazel/rules:build_make.bzl",
    "build_make",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

build_make(
    name = "make",
    srcs = ":srcs",
    configure = "configure",
    make_binary = "bin/make",
    visibility = ["//visibility:public"],
)
