# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_linux")
load("//bazel_tools:testing.bzl", "sdk_platform_test")

sdk_platform_test(
    sdk_version = "1.0.1-snapshot.20200417.3908.1.722bac90",
    platform_version = "1.0.0",
) if is_linux else None
