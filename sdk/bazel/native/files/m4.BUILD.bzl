load(
    "@//bazel/native:build_gnu_tool.bzl",
    "build_gnu_tool",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

build_gnu_tool(
    name = "m4_from_source",
    srcs = ":srcs",
    configure = "configure",
    make = "@make//:make",
    built_path = "src/m4",
    binary = "bin/m4",
    visibility = ["//visibility:public"],
)

alias(
    name = "m4",
    actual = select({
        "@platforms//os:windows": "@msys_m4//:m4",
        "//conditions:default": ":m4_from_source",
    }),
    visibility = ["//visibility:public"],
)

# Back-compat alias: consumers refer to `@m4//:m4_binary`.
alias(
    name = "m4_binary",
    actual = ":m4",
    visibility = ["//visibility:public"],
)
