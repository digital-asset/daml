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
    # Thread hermetic GNU make alongside m4 so `install_gnu_tool` uses an
    # explicit `make` binary instead of relying on host PATH.
    extra_tools = [
        "@make//:make",
        "@m4//:m4",
    ],
    pre_build_srcs = ["@autoconf//:srcs"],
    visibility = ["//visibility:public"],
)
