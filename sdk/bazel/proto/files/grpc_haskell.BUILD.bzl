# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@rules_haskell//haskell:defs.bzl", "haskell_library")
load("@stackage//:packages.bzl", "packages")

haskell_library(
    name = "grpc-haskell",
    srcs = glob(["src/**/*.hs"]),
    deps = packages["grpc-haskell"].deps,
    visibility = ["//visibility:public"],
)
