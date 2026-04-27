# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:damlc_legacy.bzl", "damlc_legacy")
load(
    "//bazel/versions:damlc_legacy.version.bzl",
    "DAMLC_LEGACY_SHA256",
    "DAMLC_LEGACY_VERSION",
)

def _impl(module_ctx):
    damlc_legacy(
        name = "damlc_legacy",
        sha256 = DAMLC_LEGACY_SHA256,
        version = DAMLC_LEGACY_VERSION,
    )

damlc_legacy_extension = module_extension(implementation = _impl)