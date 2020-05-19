# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:versions.bzl", "version_to_name")
load("//:versions.bzl", "latest_stable_version")

# Indexed first by test tool version and then by sandbox version.
# Note that at this point the granularity for disabling tests
# is sadly quite coarse. See
# https://discuss.daml.com/t/can-i-disable-individual-tests-in-the-ledger-api-test-tool/226
# for details.
excluded_test_tool_tests = {
    "1.0.0": {
        "1.1.0-snapshot.20200430.4057.0.681c862d": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.0-snapshot.20200506.4107.0.7e448d81": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.0.1-snapshot.20200424.3917.0.16093690": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.0.1": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.1": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.2.0-snapshot.20200513.4172.0.021f4af3": [
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
        "1.0.1-snapshot.20200424.3917.0.16093690": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.0.1": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.0-snapshot.20200430.4057.0.681c862d": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.0-snapshot.20200506.4107.0.7e448d81": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.1": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.2.0-snapshot.20200513.4172.0.021f4af3": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
    },
    "1.0.1-snapshot.20200424.3917.0.16093690": {
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
        "1.1.0-snapshot.20200430.4057.0.681c862d": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "1.1.0-snapshot.20200506.4107.0.7e448d81": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "1.1.1": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "1.2.0-snapshot.20200513.4172.0.021f4af3": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
    },
    "1.0.1": {
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
        "1.1.0-snapshot.20200430.4057.0.681c862d": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "1.1.0-snapshot.20200506.4107.0.7e448d81": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "1.1.1": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "1.2.0-snapshot.20200513.4172.0.021f4af3": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
        ],
    },
    "1.1.0-snapshot.20200422.3991.0.6391ee9f": {
        "1.0.1-snapshot.20200424.3917.0.16093690": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.0.1": [
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.0-snapshot.20200430.4057.0.681c862d": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.0-snapshot.20200506.4107.0.7e448d81": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.1.1": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "1.2.0-snapshot.20200513.4172.0.021f4af3": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
        "0.0.0": [
            # This restriction has been removed in https://github.com/digital-asset/daml/pull/5611.
            "ContractKeysSubmitterIsMaintainerIT",
            # Fix for https://github.com/digital-asset/daml/issues/5562
            "ContractKeysIT",
        ],
    },
    "1.1.0-snapshot.20200430.4057.0.681c862d": {
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
    "1.1.0-snapshot.20200506.4107.0.7e448d81": {
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
    "1.1.1": {
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
    "1.2.0-snapshot.20200513.4172.0.021f4af3": {
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
        daml,
        sandbox,
        sandbox_args = [],
        data = [],
        **kwargs):
    native.sh_test(
        name = name,
        # TODO[AH]: rules_haskell's runfiles library uses the runfiles tree
        # relative to the actual executable path (`getExecutablePath`) instead
        # of argument 0, as [specified][runfiles-spec]. This means that the
        # symlink generated by `sh_test` does not override the runfiles tree
        # and `data` dependencies of the `sh_test` rule are not found by the
        # test runner. This should be fixed in rules_haskell. In the meantime
        # we work around the issue by using a wrapper script that does the
        # `rlocation` lookup using the correct runfiles tree.
        # [runfiles-spec]: https://docs.google.com/document/d/e/2PACX-1vSDIrFnFvEYhKsCMdGdD40wZRBX3m3aZ5HhVj4CtHPmiXKDCxioTUbYsDydjKtFDAzER5eg7OjJWs3V/pub
        srcs = ["//bazel_tools:daml_ledger_test.sh"],
        args = [
            "$(rootpath //bazel_tools/daml_ledger:runner)",
            #"--sdk-version",
            sdk_version,
            #"--daml",
            "$(rootpath %s)" % daml,
            #"--certs",
            "bazel_tools/test_certificates",
            #"--sandbox",
            "$(rootpath %s)" % sandbox,
        ] + _concat([["--sandbox-arg", arg] for arg in sandbox_args]),
        data = data + depset(direct = [
            "//bazel_tools/daml_ledger:runner",
            "//bazel_tools/daml_ledger:test-certificates",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
        ]).to_list(),
        **kwargs
    )

def create_daml_app_test(
        name,
        daml,
        sandbox,
        json_api,
        daml_types,
        daml_react,
        daml_ledger,
        messaging_patch,
        sandbox_args = [],
        json_api_args = [],
        data = [],
        **kwargs):
    native.sh_test(
        name = name,
        # See the comment on daml_ledger_test for why
        # we need the sh_test.
        srcs = ["//bazel_tools:create_daml_app_test.sh"],
        args = [
                   "$(rootpath //bazel_tools/create-daml-app:runner)",
                   #"--daml",
                   "$(rootpath %s)" % daml,
                   #"--sandbox",
                   "$(rootpath %s)" % sandbox,
                   #"--json-api",
                   "$(rootpath %s)" % json_api,
                   "$(rootpath %s)" % daml_types,
                   "$(rootpath %s)" % daml_ledger,
                   "$(rootpath %s)" % daml_react,
                   "$(rootpath %s)" % messaging_patch,
                   "$(rootpath @nodejs//:yarn)",
                   "$(rootpath @patch_dev_env//:patch)",
               ] + _concat([["--sandbox-arg", arg] for arg in sandbox_args]) +
               _concat([["--json-api-arg", arg] for arg in json_api_args]),
        data = data + depset(direct = [
            "//bazel_tools/create-daml-app:runner",
            "@nodejs//:yarn",
            "@patch_dev_env//:patch",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
            json_api,
            daml_types,
            daml_react,
            daml_ledger,
            messaging_patch,
        ]).to_list(),
        **kwargs
    )

def sdk_platform_test(sdk_version, platform_version):
    # SDK components
    daml_assistant = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
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

    json_api = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )

    # We need to use weak seeding to avoid our tests timing out
    # if the CI machine does not have enough entropy.
    sandbox_args = ["sandbox", "--contract-id-seeding=testing-weak"]

    json_api_args = ["json-api"]

    # ledger-api-test-tool test-cases
    name = "ledger-api-test-tool-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
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
        tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
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
    ) if not is_windows else None
    # We disable the postgres tests on Windows for now since our postgres setup
    # relies on Nix. This should be fixable by getting postgres from dev-env.

    # daml-ledger test-cases
    name = "daml-ledger-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )
    daml_ledger_test(
        name = name,
        sdk_version = sdk_version,
        daml = daml_assistant,
        sandbox = sandbox,
        sandbox_args = sandbox_args,
        size = "large",
        tags = extra_tags(sdk_version, platform_version),
    )
    create_daml_app_test(
        name = "create-daml-app-{sdk_version}-platform-{platform_version}".format(sdk_version = version_to_name(sdk_version), platform_version = version_to_name(platform_version)),
        daml = daml_assistant,
        sandbox = sandbox,
        json_api = json_api,
        daml_types = "@daml-sdk-{}//:daml-types.tgz".format(sdk_version),
        daml_react = "@daml-sdk-{}//:daml-react.tgz".format(sdk_version),
        daml_ledger = "@daml-sdk-{}//:daml-ledger.tgz".format(sdk_version),
        messaging_patch = "@daml-sdk-{}//:create_daml_app.patch".format(sdk_version),
        sandbox_args = sandbox_args,
        json_api_args = json_api_args,
        size = "large",
        # Yarn gets really unhappy if it is called in parallel
        # so we mark this exclusive for now.
        tags = extra_tags(sdk_version, platform_version) + ["exclusive"],
    )
