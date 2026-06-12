load(
    "@//bazel/native:build_gnu_tool.bzl",
    "build_gnu_tool",
)

exports_files(["README"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

build_gnu_tool(
    name = "automake",
    srcs = ":srcs",
    configure = "configure",
    make = "@make//:make",
    perl = "@rules_perl//:current_toolchain",
    # Thread hermetic GNU make alongside m4 so the build uses an explicit
    # `make`/`m4` instead of relying on host PATH.
    extra_tools = [
        "@make//:make",
        "@m4//:m4",
    ],
    pre_build_srcs = ["@autoconf//:srcs"],
    visibility = ["//visibility:public"],
)
