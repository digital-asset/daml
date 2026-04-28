# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel/rules:grpcurl.bzl", "grpcurl")
load(
    "//bazel/versions:grpcurl.version.bzl",
    "GRPCURL_SHA256",
    "GRPCURL_VERSION",
)

def _impl(module_ctx):
    grpcurl(
        name = "grpcurl",
        sha256 = GRPCURL_SHA256,
        version = GRPCURL_VERSION,
    )

grpcurl_extension = module_extension(implementation = _impl)
