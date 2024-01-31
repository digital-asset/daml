# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:testing.bzl", "daml_lf_compatible", "sdk_platform_test")
load(
    "//bazel_tools/daml_script:daml_script.bzl",
    "daml_script_example_dar",
    "daml_script_example_test",
)
load(
    "//bazel_tools/data_dependencies:data_dependencies.bzl",
    "data_dependencies_coins",
    "data_dependencies_daml_script_test",
    "data_dependencies_upgrade_test",
)
load("//bazel_tools:versions.bzl", "versions")
load("//:versions.bzl", "platform_versions", "sdk_versions", "stable_versions")
load("@daml//bazel_tools:haskell.bzl", "da_haskell_binary")

# This dummy tests ensure that bazel won't fail with "No test targets were
# found, yet testing was requested" when no target matches the test filter.
genrule(
    name = "dummy_test",
    outs = ["dummy_test_result.sh"],
    cmd = "touch $(OUTS)",
)

[
    sh_test(
        name = "dummy_test_sh{}".format(suffix),
        srcs = ["dummy_test_result.sh"],
        tags = tags,
    )
    for (tags, suffix) in [
        (
            ["head-quick"],
            "_head_quick",
        ),
        ([], ""),
    ]
]

config_setting(
    name = "ghci_data",
    define_values = {
        "ghci_data": "True",
    },
)

[
    sh_test(
        name = "test-daml-version-and-install-{}".format(sdk_version),
        srcs = ["test-daml-version-and-install.sh"],
        args = [sdk_version],
        data = [
            "@daml-sdk-{}//:daml".format(sdk_version),
            "@diffutils_nix//:bin/diff",
        ],
        deps = ["@bazel_tools//tools/bash/runfiles"],
    )
    for sdk_version in sdk_versions
    if versions.is_at_least("3.0.0", sdk_version)
] if not is_windows else None

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
    if versions.is_at_least("3.0.0", old_sdk_version)
]

os_name = "linux" if not is_windows else "windows"

command_targets = {
    "install_from_version": [
        "0.0.0",
    ],
    "build_from_version": [
        "0.0.0",
    ],
    "install_with_custom_version_and_build": [
        "0.0.0",
    ],
    "install_and_build_from_tarball": [
    ],
}

command_post_failure_behaviours = {
    "install_from_version": [
        "do_nothing",
    ],
    "build_from_version": [
        "do_nothing",
    ],
    "install_with_custom_version_and_build": [
        "do_nothing",
    ],
    "install_and_build_from_tarball": [
        "allow_nonrelease",
        "update_cache",
        "install_via_custom_version",
        "do_nothing",
    ],
}

[
    sh_test(
        name = "test-all-installs-{}-{}-{}-{}".format(
            command[:10],
            command_target.split("/")[0][:11],
            version_cache_behaviour[:8],
            post_failure_behaviour[:5],
        ),
        size = "enormous",
        srcs = ["test-all-installs.sh"],
        args = [
            os_name,
            command,
            command_target,
            version_cache_behaviour,
            post_failure_behaviour,
        ],
        data = [
            ":new_cache",
            ":old_cache",
            ":releases-github-api.json",
            "@head_sdk//:daml" if not is_windows else "@head_sdk//:daml.exe",
            "@head_sdk//:sdk-release-tarball-ce.tar.gz",
        ],
        deps = ["@bazel_tools//tools/bash/runfiles"],
    )
    for command in command_targets.keys()
    for command_target in command_targets[command]
    for version_cache_behaviour in [
        "init_new_cache",
        "init_old_cache",
        "no_cache_override_github_endpoint",
    ]
    for post_failure_behaviour in command_post_failure_behaviours[command]
]
