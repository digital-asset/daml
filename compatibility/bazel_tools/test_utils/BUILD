# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools:haskell.bzl",
    "da_haskell_library",
)

da_haskell_library(
    name = "test_utils",
    srcs = glob(["DA/Test/**/*.hs"]),
    hackage_deps = [
        "base",
        "bytestring",
        "tar-conduit",
        "conduit",
        "directory",
        "extra",
        "filepath",
        "network",
        "process",
        "safe-exceptions",
        "tasty",
        "tasty-hunit",
        "text",
        "unix-compat",
    ],
    visibility = ["//visibility:public"],
    deps = [
    ],
)
