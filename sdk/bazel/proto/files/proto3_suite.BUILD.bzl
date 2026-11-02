# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")

# XXX: haskell_cabal_binary inexplicably fails with
#   realgcc.exe: error: CreateProcess: No such file or directory
# So we use haskell_binary instead.
load("@rules_haskell//haskell:defs.bzl", "haskell_binary")
load("@stackage//:packages.bzl", "packages")

deps = [p for p in packages["proto3-suite"].deps if p != "swagger2"]
haskell_cabal_library(
    name = "proto3-suite",
    version = packages["proto3-suite"].version,
    srcs = glob(["src/**", "test-files/*.bin", "tests/*", "proto3-suite.cabal"]),
    haddock = False,
    deps = deps,
    verbose = False,
    visibility = ["//visibility:public"],
    flags = ["-swagger"],
)

haskell_binary(
    name = "compile-proto-file",
    srcs = ["tools/compile-proto-file/Main.hs"],
    ghcopts = ["-w", "-optF=-w"],
    deps = [":proto3-suite"] + deps,
    visibility = ["//visibility:public"],
)
