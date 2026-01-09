# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:scala_version.bzl", "scala_version_configure")
load("//bazel_tools:os_info.bzl", "os_info")
load("//bazel_tools:build_environment.bzl", "build_environment")

def _scala_extension_impl(module_ctx):
    scala_version_configure(name = "scala_version")
    os_info(name = "os_info")
    build_environment(name = "build_environment")

scala_extension = module_extension(
    implementation = _scala_extension_impl,
)