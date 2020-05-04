# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:versions.bzl", "versions")
load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:testing.bzl", "sdk_platform_test")
load(
    "//bazel_tools/daml_script:daml_script.bzl",
    "daml_script_dar",
    "daml_script_test",
)

config_setting(
    name = "ghci_data",
    define_values = {
        "ghci_data": "True",
    },
)

sdk_versions = [
    "1.0.0",
    "1.0.1-snapshot.20200417.3908.1.722bac90",
    "1.1.0-snapshot.20200422.3991.0.6391ee9f",
    "0.0.0",
]

platform_versions = [
    "1.0.0",
    "1.0.1-snapshot.20200417.3908.1.722bac90",
    "1.1.0-snapshot.20200422.3991.0.6391ee9f",
    "0.0.0",
]

[
    sh_binary(
        name = "sandbox-with-postgres-{}".format(version),
        srcs = ["@//bazel_tools:sandbox-with-postgres.sh"],
        data = [
            "@//bazel_tools/client_server:with-postgres",
            "@daml-sdk-{}//:daml".format(version),
        ],
        deps = ["@bazel_tools//tools/bash/runfiles"],
    )
    for version in platform_versions
]

[
    sdk_platform_test(
        platform_version = platform_version,
        sdk_version = sdk_version,
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
]

[
    daml_script_dar(sdk_version)
    for sdk_version in sdk_versions
]

[
    daml_script_test(sdk_version, platform_version)
    for sdk_version in sdk_versions
    for platform_version in platform_versions
    # Test that the DAML script runner can run DARs built with an older SDK
    # version. I.e. where the runner version is at least the SDK version or
    # more recent. The HEAD version 0.0.0 is a special case.
    if versions.is_at_least(sdk_version, platform_version) and sdk_version != "0.0.0"
       or platform_version == "0.0.0"
]

test_suite(
    name = "head-quick",
    tags = ["head-quick"],
)
