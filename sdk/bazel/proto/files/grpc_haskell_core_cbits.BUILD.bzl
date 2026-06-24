load("@daml-sdk//bazel_tools:cc_bundle.bzl", "cc_bundle")

cc_bundle(
    name = "merged_cbits",
    lib = "cbits",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "cbits",
    srcs = glob(["cbits/*.c"]),
    hdrs = glob(["include/*.h"]),
    includes = ["include/"],
    deps = [
        "@grpc//:grpc",
    ],
)
