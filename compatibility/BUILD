# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_linux")
load("//bazel_tools:testing.bzl", "sdk_platform_test")

sdk_versions = [
    "1.0.0",
    "1.0.1-snapshot.20200417.3908.1.722bac90",
]

platform_versions = [
    "1.0.0",
    "1.0.1-snapshot.20200417.3908.1.722bac90",
]

[
    sdk_platform_test(
        sdk_version = sdk_version,
        platform_version = platform_version,
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
] if is_linux else None
