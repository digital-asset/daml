# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@daml//bazel_tools:haskell.bzl", "da_haskell_test")

da_haskell_test(
    name = "assistant-platform-version",
    srcs = glob(["src/**/*.hs"]),
    data = [
        "@daml-sdk-tarball-latest-stable//file:downloaded",
        "@head_sdk//:sdk-release-tarball-ce.tar.gz",
    ],
    hackage_deps = [
        "base",
        "tar-conduit",
        "conduit",
        "conduit-extra",
        "text",
        "filepath",
        "directory",
        "extra",
        "process",
        "typed-process",
        "tasty",
        "tasty-hunit",
        "utf8-string",
        "stm",
    ],
    main_function = "DA.Test.PlatformVersion.main",
    tags = [
        "exclusive",
        "head-quick",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//bazel_tools:versions-hs-lib",
        "//bazel_tools/daml_ledger:sandbox-helper",
        "//bazel_tools/test_utils",
        "@rules_haskell//tools/runfiles",
    ],
)
