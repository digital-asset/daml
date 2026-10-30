# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")
load("//compatibility:versions.bzl", "platform_versions", "sdk_versions")
load("//compatibility/bazel_tools:sdks.bzl", "HEAD_VERSION")
load(
    "//compatibility/bazel_tools:testing.bzl",
    "compatibility_test_suites",
    "daml_lf_compatible",
    "sdk_platform_test",
)
load("//compatibility/bazel_tools:versions.bzl", "versions")
load(
    "//compatibility/bazel_tools/daml_script:daml_script.bzl",
    "daml_script_example_dar",
    "daml_script_example_test",
)
load(
    "//compatibility/bazel_tools/data_dependencies:data_dependencies.bzl",
    "data_dependencies_coins",
    "data_dependencies_daml_script_test",
    "data_dependencies_upgrade_test",
)

[
    sdk_platform_test(
        platform_version = platform_version,
        sdk_version = HEAD_VERSION,
    )
    for platform_version in platform_versions
    if platform_version != HEAD_VERSION and daml_lf_compatible(HEAD_VERSION, platform_version)
]

[
    sdk_platform_test(
        platform_version = HEAD_VERSION,
        sdk_version = sdk_version,
    )
    for sdk_version in sdk_versions
    if sdk_version != HEAD_VERSION and daml_lf_compatible(sdk_version, HEAD_VERSION)
]

[
    daml_script_example_dar(sdk_version)
    for sdk_version in sdk_versions
]

daml_script_example_dar(HEAD_VERSION, "//daml-script/runner:daml-script-no-failure-status-oci")

[
    daml_script_example_test(
        compiler_version = sdk_version,
        runner_version = platform_version,
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
    if versions.is_at_least(sdk_version, platform_version)
] if not is_windows else None

[
    data_dependencies_coins(
        sdk_version = sdk_version,
    )
    for sdk_version in sdk_versions
]

[
    data_dependencies_upgrade_test(
        new_sdk_version = new_sdk_version,
        old_sdk_version = old_sdk_version,
    )
    for old_sdk_version in sdk_versions
    for new_sdk_version in sdk_versions
    if versions.is_at_least(old_sdk_version, new_sdk_version)
]

[
    data_dependencies_daml_script_test(
        old_sdk_version = old_sdk_version,
    )
    for old_sdk_version in sdk_versions
    if old_sdk_version != HEAD_VERSION
]

data_dependencies_daml_script_test(
    HEAD_VERSION,
    run_unknown_failure_test = True,
)

compatibility_test_suites()
