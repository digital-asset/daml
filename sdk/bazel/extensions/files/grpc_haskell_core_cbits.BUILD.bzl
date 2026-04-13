load("@daml-sdk//bazel_tools:fat_cc_library.bzl", "fat_cc_library")

fat_cc_library(
    name = "merged_cbits",
    input_lib = "cbits",
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
