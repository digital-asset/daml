# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:testing.bzl", "create_daml_app_codegen", "create_daml_app_dar", "daml_lf_compatible", "sdk_platform_test")
load(
    "//bazel_tools/daml_script:daml_script.bzl",
    "daml_script_example_dar",
    "daml_script_example_test",
)
load(
    "//bazel_tools/daml_trigger:daml_trigger.bzl",
    "daml_trigger_dar",
    "daml_trigger_test",
)
load(
    "//bazel_tools/data_dependencies:data_dependencies.bzl",
    "data_dependencies_coins",
    "data_dependencies_daml_script_test",
    "data_dependencies_upgrade_test",
)
load("//bazel_tools:versions.bzl", "versions")
load("//sandbox-migration:util.bzl", "migration_test")
load("//:versions.bzl", "platform_versions", "sdk_versions", "stable_versions")
load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary")

config_setting(
    name = "ghci_data",
    define_values = {
        "ghci_data": "True",
    },
)

[
    sh_binary(
        name = "sandbox-with-postgres-{}".format(version),
        srcs = ["@//bazel_tools:sandbox-with-postgres.sh"],
        args = ["sandbox"],
        data = [
            "@//bazel_tools/client_server/with-postgres:with-postgres-exe",
            "@daml-sdk-{}//:daml".format(version),
        ] + ([
            "@daml-sdk-{}//:sandbox-on-x".format(version),
        ] if versions.is_at_least("2.0.0", version) else []),
        deps = ["@bazel_tools//tools/bash/runfiles"],
    )
    for version in platform_versions
]

[
    [
        create_daml_app_dar(sdk_version),
        create_daml_app_codegen(sdk_version),
    ]
    for sdk_version in sdk_versions
]

# Instead of testing the full cartesian product of all SDK versions with
# all platform (~= Sandbox/JSON API) versions, we test the latest version of
# each with all versions of the other. This gives us a reasonable feedback
# with regards to maintaining backwards-compatibility without causing the
# test runs to grow quadratically.
head = "0.0.0"

# Missing on purpose: do not test the latest SDK with the latest platform
# That is not a compatibility test, it's just testing the main branch. ;)

# Test all old platform versions with the latest SDK
[
    sdk_platform_test(
        platform_version = platform_version,
        sdk_version = head,
    )
    for platform_version in platform_versions
    if platform_version != head and daml_lf_compatible(head, platform_version)
]

# Test all old SDK versions with the latest platform
[
    sdk_platform_test(
        platform_version = head,
        sdk_version = sdk_version,
    )
    for sdk_version in sdk_versions
    if sdk_version != head and daml_lf_compatible(sdk_version, head)
]

[
    daml_script_example_dar(sdk_version)
    for sdk_version in sdk_versions
]

[
    daml_script_example_test(
        compiler_version = sdk_version,
        runner_version = platform_version,
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
    # Test that the Daml script runner can run DARs built with an older SDK
    # version. I.e. where the runner version is at least the SDK version or
    # more recent.
    if versions.is_at_least(sdk_version, platform_version)
] if not is_windows else None

# Change to `CommandId` generation
first_post_7587_trigger_version = "1.7.0-snapshot.20201012.5405.0.af92198d"

[
    daml_trigger_dar(sdk_version)
    for sdk_version in sdk_versions
    if versions.is_at_least(first_post_7587_trigger_version, sdk_version)
]

[
    daml_trigger_test(
        compiler_version = sdk_version,
        runner_version = platform_version,
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
    # Test that the Daml trigger runner can run DARs built with an older SDK
    # version. I.e. where the runner version is at least the SDK version or
    # more recent.
    if versions.is_at_least(first_post_7587_trigger_version, sdk_version) and
       versions.is_at_least(sdk_version, platform_version)
]

migration_test(
    name = "migration",
    timeout = "eternal",
    quick_tags = ["head-quick"],
    # Exclusive due to hardcoded postgres ports.
    tags = [
        "exclusive",
    ],
    versions = [
        version
        for version in stable_versions
        # We skip versions before 1.6.0. These versions had a bug where ledger
        # effective time was stored at higher precision than it should have
        # been. See https://github.com/digital-asset/daml/pull/11512#discussion_r751041534
        if versions.is_at_least("1.6.0", version)
    ],
) if not is_windows else None

migration_test(
    name = "meta-migration-test",
    quick_tags = [],
    tags = [
        "exclusive",
        "manual",
    ],
    versions = [
        "1.0.0",
        "0.0.0",
    ],
)

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
    # Tests that we can build a package with a newer SDK version that has
    # data-dependencies on packages built with an older SDK version.
    if versions.is_at_least(old_sdk_version, new_sdk_version)
]

[
    data_dependencies_daml_script_test(
        old_sdk_version = old_sdk_version,
    )
    for old_sdk_version in sdk_versions
    # Tests that we can run a daml script from a package built with an older
    # SDK version even when exposed through an import in a package built with
    # the latest SDK version.
    # (regression test for https://github.com/digital-asset/daml/issues/14291)
    if old_sdk_version != "0.0.0"
    # These tests use data-dependency reexports (#11147), so the earliest
    # supported SDK is 1.18.0
    if versions.is_at_least("1.18.0", old_sdk_version)
]
