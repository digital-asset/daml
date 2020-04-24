# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)

latest_stable = "1.0.0"

# Indexed first by test tool version and then by sandbox version.
# Note that at this point the granularity for disabling tests
# is sadly quite coarse. See
# https://discuss.daml.com/t/can-i-disable-individual-tests-in-the-ledger-api-test-tool/226
# for details.
excluded_test_tool_tests = {
    "1.0.0": {
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
    },
    "1.0.1-snapshot.20200417.3908.1.722bac90": {
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
    },
    "1.1.0-snapshot.20200422.3991.0.6391ee9f": {
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
    },
    "0.0.0": {
        "1.0.0": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.0.1-snapshot.20200417.3908.1.722bac90": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.0-snapshot.20200422.3991.0.6391ee9f": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
    },
}

def get_excluded_tests(test_tool_version, sandbox_version):
    return excluded_test_tool_tests.get(test_tool_version, default = {}).get(sandbox_version, default = [])

def extra_tags(sdk_version, platform_version):
    if sorted([sdk_version, platform_version]) == sorted(["0.0.0", latest_stable]):
        # These tests are the ones that we check on each PR since they
        # are the most useful ones and hopefully fast enough.
        return ["head-quick"]
    return []

def sdk_platform_test(sdk_version, platform_version):
    # SDK components
    ledger_api_test_tool = "@daml-sdk-{sdk_version}//:ledger-api-test-tool".format(
        sdk_version = sdk_version,
    )
    dar_files = "@daml-sdk-{sdk_version}//:dar-files".format(
        sdk_version = sdk_version,
    )

    # Platform components
    sandbox = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )
    sandbox_args = ["sandbox"]

    # Test case
    name = "ledger-api-test-tool-{sdk_version}-platform-{platform_version}".format(
        sdk_version = sdk_version,
        platform_version = platform_version,
    )
    exclusions = ["--exclude=" + test for test in get_excluded_tests(test_tool_version = sdk_version, sandbox_version = platform_version)]
    client_server_test(
        name = name,
        client = ledger_api_test_tool,
        client_args = [
            "localhost:6865",
            "--open-world",
            "--exclude=ClosedWorldIT",
        ] + exclusions,
        data = [dar_files],
        runner = "@//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = sandbox,
        server_args = sandbox_args,
        server_files = ["$(rootpaths {dar_files})".format(
            dar_files = dar_files,
        )],
        tags = ["exclusive"] + extra_tags(sdk_version, platform_version),
    )

    client_server_test(
        name = name + "-postgresql",
        client = ledger_api_test_tool,
        client_args = [
            "localhost:6865",
            "--open-world",
            "--exclude=ClosedWorldIT",
        ] + exclusions,
        data = [dar_files],
        runner = "@//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = ":sandbox-with-postgres-{}".format(platform_version),
        server_args = [platform_version],
        server_files = ["$(rootpaths {dar_files})".format(
            dar_files = dar_files,
        )],
        tags = ["exclusive"] + extra_tags(sdk_version, platform_version),
    )
