# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:versions.bzl", "version_to_name", "versions")
load("//:versions.bzl", "latest_stable_version")

# Range of test-tool versions version and then a nested list of ranges
# of platform versions and their corresponding exclusions.
# Note that before 1.3 the granularity for disabling tests
# was sadly quite coarse. See
# https://discuss.daml.com/t/can-i-disable-individual-tests-in-the-ledger-api-test-tool/226
# for details.
# PRs that resulted in exclusions:
# - ContractKeysIT:
#   - https://github.com/digital-asset/daml/pull/5608
# - ContractKeysSubmitterIsMaintainerIT:
#   - https://github.com/digital-asset/daml/pull/5611
excluded_test_tool_tests = [
    {
        "start": "1.0.0",
        "end": "1.0.0",
        "platform_ranges": [
            {
                "start": "1.0.1-snapshot.20200424.3917.0.16093690",
                "end": "1.0.1",
                "exclusions": ["ContractKeysIT"],
            },
            {
                "start": "1.1.0-snapshot.20200430.4057.0.681c862d",
                "exclusions": ["ContractKeysIT", "ContractKeysSubmitterIsMaintainerIT"],
            },
        ],
    },
    {
        "start": "1.0.1",
        "end": "1.0.1",
        "platform_ranges": [
            {
                "end": "1.0.1-snapshot.20200417.3908.1.722bac90",
                "exclusions": ["ContractKeysIT"],
            },
            {
                "start": "1.1.0-snapshot.20200430.4057.0.681c862d",
                "exclusions": ["ContractKeysSubmitterIsMaintainerIT"],
            },
        ],
    },
    {
        "start": "1.1.1",
        "end": "1.3.0-snapshot.20200617.4484.0.7e0a6848",
        "platform_ranges": [
            {
                "end": "1.0.1-snapshot.20200417.3908.1.722bac90",
                "exclusions": ["ContractKeysIT"],
            },
        ],
    },
    {
        "start": "1.3.0-snapshot.20200623.4546.0.4f68cfc4",
        "platform_ranges": [
            {
                "end": "1.0.1-snapshot.20200417.3908.1.722bac90",
                "exclusions": [
                    "ContractKeysIT:CKFetchOrLookup",
                    "ContractKeysIT:CKNoFetchUndisclosed",
                ],
            },
        ],
    },
    {
        "end": "1.3.0-snapshot.20200617.4484.0.7e0a6848",
        "platform_ranges": [
            {
                "start": "1.3.0-snapshot.20200701.4616.0.bdbefd11",
                "exclusions": [
                    "CommandServiceIT",
                ],
            },
        ],
    },
    {
        "start": "1.5.0-snapshot.20200902.5118.0.2b3cf1b3",
        "platform_ranges": [
            {
                "start": "1.0.0",
                "end": "1.3.0",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/7251
                    "CommandSubmissionCompletionIT:CSCAfterEnd",
                    "TransactionServiceIT:TXAfterEnd",
                    "TransactionServiceIT:TXTreesAfterEnd",
                ],
            },
        ],
    },
    {
        "end": "1.3.0-snapshot.20200617.4484.0.7e0a6848",
        "platform_ranges": [
            {
                "start": "1.6.0-snapshot.20200922.5258.0.cd4a06db",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/7400
                    "WronglyTypedContractIdIT",
                ],
            },
        ],
    },
    {
        "start": "1.3.0-snapshot.20200623.4546.0.4f68cfc4",
        "end": "1.6.0-snapshot.20200915.5208.0.09014dc6",
        "platform_ranges": [
            {
                "start": "1.6.0-snapshot.20200922.5258.0.cd4a06db",
                "exclusions": [
                    # See https://github.com/digital-asset/daml/pull/7400
                    "WronglyTypedContractIdIT:WTFetchFails",
                ],
            },
        ],
    },
]

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

def get_excluded_tests(test_tool_version, sandbox_version):
    exclusions = []
    for test_tool_range in excluded_test_tool_tests:
        if in_range(test_tool_version, test_tool_range):
            for platform_range in test_tool_range["platform_ranges"]:
                if in_range(sandbox_version, platform_range):
                    exclusions += platform_range["exclusions"]
    return exclusions

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
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = data + depset(direct = [
            "//bazel_tools/daml_ledger:runner",
            "//bazel_tools/daml_ledger:test-certificates",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
        ]).to_list(),
        **kwargs
    )

def create_daml_app_dar(sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    messaging_patch = "@daml-sdk-{sdk_version}//:create_daml_app.patch".format(
        sdk_version = sdk_version,
    )
    native.genrule(
        name = "create-daml-app-dar-{sdk_version}".format(
            sdk_version = version_to_name(sdk_version),
        ),
        outs = ["create-daml-app-{sdk_version}.dar".format(
            sdk_version = version_to_name(sdk_version),
        )],
        srcs = [messaging_patch],
        tools = [daml, "@patch_dev_env//:patch"],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
$(execpath {daml}) new $$TMP_DIR/create-daml-app create-daml-app
$(execpath @patch_dev_env//:patch) -s -d $$TMP_DIR/create-daml-app -p2 < $(execpath {messaging_patch})
$(execpath {daml}) build --project-root=$$TMP_DIR/create-daml-app -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            sdk_version = sdk_version,
            messaging_patch = messaging_patch,
        ),
    )

def create_daml_app_codegen(sdk_version):
    daml = "@daml-sdk-{}//:daml".format(sdk_version)
    daml_types = "@daml-sdk-{}//:daml-types.tgz".format(sdk_version)
    daml_ledger = "@daml-sdk-{}//:daml-ledger.tgz".format(sdk_version)
    dar = "//:create-daml-app-{}.dar".format(version_to_name(sdk_version))
    native.genrule(
        name = "create-daml-app-codegen-{}".format(version_to_name(sdk_version)),
        outs = ["create-daml-app-codegen-{}.tar.gz".format(version_to_name(sdk_version))],
        srcs = [dar, daml_types, daml_ledger],
        tools = [
            daml,
            "//bazel_tools/create-daml-app:run-with-yarn",
            "@daml//bazel_tools/sh:mktgz",
        ],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
mkdir -p $$TMP_DIR/daml-types $$TMP_DIR/daml-ledger
tar xf $(rootpath {daml_types}) --strip-components=1 -C $$TMP_DIR/daml-types
tar xf $(rootpath {daml_ledger}) --strip-components=1 -C $$TMP_DIR/daml-ledger
$(execpath //bazel_tools/create-daml-app:run-with-yarn) $$TMP_DIR $$PWD/$(execpath {daml}) codegen js -o $$TMP_DIR/daml.js $(execpath {dar})
rm -rf $$TMP_DIR/daml.js/node_modules
$(execpath @daml//bazel_tools/sh:mktgz) $@ -C $$TMP_DIR daml.js
""".format(
            daml = daml,
            daml_types = daml_types,
            daml_ledger = daml_ledger,
            dar = dar,
            sdk_version = sdk_version,
        ),
    )

def create_daml_app_test(
        name,
        daml,
        sandbox,
        sandbox_version,
        json_api,
        json_api_version,
        daml_types,
        daml_react,
        daml_ledger,
        messaging_patch,
        codegen_output,
        dar,
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
            sandbox_version,
            #"--json-api",
            "$(rootpath %s)" % json_api,
            json_api_version,
            "$(rootpath %s)" % daml_types,
            "$(rootpath %s)" % daml_ledger,
            "$(rootpath %s)" % daml_react,
            "$(rootpath %s)" % messaging_patch,
            "$(rootpath @nodejs//:npm_bin)",
            "$(rootpath @nodejs//:node)",
            "$(rootpath @patch_dev_env//:patch)",
            "$(rootpath //bazel_tools/create-daml-app:testDeps.json)",
            "$(rootpath //bazel_tools/create-daml-app:index.test.ts)",
            "$(rootpath %s)" % codegen_output,
            "$(rootpath %s)" % dar,
        ],
        data = data + depset(direct = [
            "//bazel_tools/create-daml-app:runner",
            "@nodejs//:npm_bin",
            "@nodejs//:node",
            "@patch_dev_env//:patch",
            "//bazel_tools/create-daml-app:testDeps.json",
            "//bazel_tools/create-daml-app:index.test.ts",
            # Deduplicate if daml and sandbox come from the same release.
            daml,
            sandbox,
            json_api,
            daml_types,
            daml_react,
            daml_ledger,
            messaging_patch,
            codegen_output,
            dar,
        ]).to_list(),
        deps = [
            "@bazel_tools//tools/bash/runfiles",
        ],
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

    sandbox_classic_args = ["sandbox-classic"]

    json_api_args = ["json-api"]

    # --implicit-party-allocation=false only exists in SDK >= 1.2.0 so
    # for older versions we still have to disable ClosedWorldIT
    (extra_sandbox_next_args, extra_sandbox_next_exclusions) = (["--implicit-party-allocation=false"], []) if versions.is_at_least("1.2.0", platform_version) else ([], ["--exclude=ClosedWorldIT"])

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
        ] + exclusions + extra_sandbox_next_exclusions,
        data = [dar_files],
        runner = "@//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = sandbox,
        server_args = sandbox_args + extra_sandbox_next_args,
        server_files = ["$(rootpaths {dar_files})".format(
            dar_files = dar_files,
        )],
        tags = ["exclusive", sdk_version, platform_version] + extra_tags(sdk_version, platform_version),
    )

    client_server_test(
        name = name + "-classic",
        client = ledger_api_test_tool,
        client_args = [
            "localhost:6865",
            "--exclude=ClosedWorldIT",
        ] + exclusions,
        data = [dar_files],
        runner = "@//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = sandbox,
        server_args = sandbox_classic_args,
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
        ] + exclusions + extra_sandbox_next_exclusions,
        data = [dar_files],
        runner = "@//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = ":sandbox-with-postgres-{}".format(platform_version),
        server_args = [platform_version] + sandbox_args + extra_sandbox_next_args,
        server_files = ["$(rootpaths {dar_files})".format(
            dar_files = dar_files,
        )],
        tags = ["exclusive"] + extra_tags(sdk_version, platform_version),
    ) if not is_windows else None

    client_server_test(
        name = name + "-classic-postgresql",
        client = ledger_api_test_tool,
        client_args = [
            "localhost:6865",
            "--exclude=ClosedWorldIT",
        ] + exclusions,
        data = [dar_files],
        runner = "@//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = ":sandbox-with-postgres-{}".format(platform_version),
        server_args = [platform_version, "sandbox-classic"],
        server_files = ["$(rootpaths {dar_files})".format(
            dar_files = dar_files,
        )],
        tags = ["exclusive"] + extra_tags(sdk_version, platform_version),
    ) if not is_windows else None

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
        # We see timeouts here fairly regularly so we
        # increase the number of CPUs.
        tags = ["cpu:2"] + extra_tags(sdk_version, platform_version),
    )

    # For now, we only cover the DABL usecase where
    # sandbox and the JSON API come from the same SDK.
    # However, the test setup is flexible enough, that we
    # can control them individually.
    create_daml_app_test(
        name = "create-daml-app-{sdk_version}-platform-{platform_version}".format(sdk_version = version_to_name(sdk_version), platform_version = version_to_name(platform_version)),
        daml = daml_assistant,
        sandbox = sandbox,
        sandbox_version = platform_version,
        json_api = json_api,
        json_api_version = platform_version,
        daml_types = "@daml-sdk-{}//:daml-types.tgz".format(sdk_version),
        daml_react = "@daml-sdk-{}//:daml-react.tgz".format(sdk_version),
        daml_ledger = "@daml-sdk-{}//:daml-ledger.tgz".format(sdk_version),
        messaging_patch = "@daml-sdk-{}//:create_daml_app.patch".format(sdk_version),
        codegen_output = "//:create-daml-app-codegen-{}.tar.gz".format(version_to_name(sdk_version)),
        dar = "//:create-daml-app-{}.dar".format(version_to_name(sdk_version)),
        size = "large",
        # Yarn gets really unhappy if it is called in parallel
        # so we mark this exclusive for now.
        tags = extra_tags(sdk_version, platform_version) + ["exclusive"],
    )
