# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//compatibility:versions.bzl", "latest_stable_version")
load("//compatibility/bazel_tools:sdks.bzl", "COMPAT_TAGS", "daml_sdk")
load("//compatibility/bazel_tools:versions.bzl", "version_to_name", "versions")

def in_range(version, range):
    start = range.get("start")
    end = range.get("end")
    if start and not versions.is_at_least(start, version):
        # Before start
        return False
    if end and not versions.is_at_most(end, version):
        # After end
        return False
    return True

def extra_tags(sdk_version, platform_version):
    if sorted([sdk_version, platform_version]) == sorted(["0.0.0", latest_stable_version]):
        # These tests are the ones that we check on each PR since they
        # are the most useful ones and hopefully fast enough.
        return ["head-quick"]
    return []

def _concat(lists):
    return [v for l in lists for v in l]

def daml_ledger_test(
        name,
        sdk_version,
        platform_version,
        daml,
        sandbox,
        sandbox_args = [],
        data = [],
        tags = [],
        **kwargs):
    # We wrap it in an SH test to pass different arguments.
    native.sh_test(
        name = name,
        srcs = ["//compatibility/bazel_tools:daml_ledger_test.sh"],
        args = [
            "$(rlocationpath //compatibility/bazel_tools/daml_ledger:runner)",
            # "--sdk",
            "$(rlocationpath %s)" % daml,
            # "--platform",
            "$(rlocationpath %s)" % sandbox,
            "--sdk-version",
            sdk_version,
            "--platform-version",
            platform_version,
        ] + _concat([["--sandbox-arg", arg] for arg in sandbox_args]),
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = data + depset(direct = [
            "//compatibility/bazel_tools/daml_ledger:runner",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
        ]).to_list(),
        tags = COMPAT_TAGS + tags,
        **kwargs
    )

# FIXME
#
# SDK components may default to a LF version too recent for a given platform version.
#
# This predicate can be used to filter sdk_platform_test rules as a temporary
# measure to prevent spurious errors on CI.
#
# The proper fix is to use the appropriate version of Daml-LF for every SDK/platform pair.

def daml_lf_compatible(sdk_version, platform_version):
    return (
        # any post 3.4 platform supports any 3x SDK
        in_range(platform_version, {"start": "3.4.0-snapshot"}) and in_range(sdk_version, {"start": "3.0.0-snapshot"})
    )

def sdk_platform_test(sdk_version, platform_version):
    # SDK components
    daml_assistant = daml_sdk(sdk_version)

    # Platform components
    canton_sandbox = daml_sdk(platform_version)
    canton_sandbox_args = ["sandbox", "--canton-port-file", "__PORTFILE__"]

    # daml-ledger test-cases
    name = "daml-ledger-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )

    daml_ledger_test(
        name = name,
        sdk_version = sdk_version,
        platform_version = platform_version,
        daml = daml_assistant,
        sandbox = canton_sandbox,
        sandbox_args = canton_sandbox_args,
        size = "large",
        # We see timeouts here fairly regularly so we
        # increase the number of CPUs.
        tags = ["cpu:2"] + extra_tags(sdk_version, platform_version),
    )

def compatibility_test_suites():
    tests = [
        rule
        for rule in native.existing_rules().values()
        if rule["kind"].endswith("_test") and "compatibility" in rule["tags"]
    ]
    native.test_suite(
        name = "compatibility",
        tags = ["manual"],
        tests = [":" + test["name"] for test in tests],
    )
    native.test_suite(
        name = "quick",
        tags = ["manual"],
        tests = [":" + test["name"] for test in tests if "head-quick" in test["tags"]],
    )
