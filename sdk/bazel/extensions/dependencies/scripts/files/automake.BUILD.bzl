load(
    "@//bazel/rules:install_gnu_tool.bzl",
    "install_gnu_tool",
)

exports_files(["README"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

install_gnu_tool(
    name = "automake",
    srcs = ":srcs",
    configure = "configure",
    perl = "@rules_perl//:current_toolchain",
    extra_tools = ["@m4//:m4"],
    pre_build_srcs = ["@autoconf//:srcs"],
    visibility = ["//visibility:public"],
)
