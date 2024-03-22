# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//bazel_tools:scala.bzl",
    "lf_scalacopts",
)

# Shared options for Ledger Clients code
hj_scalacopts = lf_scalacopts + [
    "-Xlint:implicit-recursion",
    "-Xlint:nonlocal-return",
    "-Xlint:nullary-unit",
    "-P:wartremover:traverser:org.wartremover.warts.NonUnitStatements",
]
