# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

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
    linkopts = select({
        "@platforms//os:windows": [
            "-lws2_32",
            "-lcrypt32",
            "-lbcrypt",
            "-ldbghelp",
        ],
        "//conditions:default": [],
    }),
    deps = [
        "@grpc//:grpc",
    ],
)
