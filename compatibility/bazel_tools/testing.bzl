# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_linux", "is_windows")
load("//bazel_tools:versions.bzl", "version_to_name", "versions")
load("//:versions.bzl", "latest_stable_version")

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
        daml,
        sandbox,
        sandbox_args = [],
        data = [],
        **kwargs):
    # We wrap it in an SH test to pass different arguments.
    native.sh_test(
        name = name,
        srcs = ["//bazel_tools:daml_ledger_test.sh"],
        args = [
            "$(rootpath //bazel_tools/daml_ledger:runner)",
            # "--daml",
            "$(rootpath %s)" % daml,
            # "--sandbox",
            "$(rootpath %s)" % sandbox,
            "--sdk-version",
            sdk_version,
        ] + _concat([["--sandbox-arg", arg] for arg in sandbox_args]),
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = data + depset(direct = [
            "//bazel_tools/daml_ledger:runner",
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
        # any platform supports any pre 1.11 SDK
        not in_range(sdk_version, {"start": "1.11.0-snapshot"})
    ) or (
        # any post 1.10.0 platform supports any pre 1.12 SDK
        in_range(platform_version, {"start": "1.10.0-snapshot"}) and not in_range(sdk_version, {"start": "1.12.0-snapshot"})
    ) or (
        # any post 1.10.0 platform supports any pre 1.14 SDK
        in_range(platform_version, {"start": "1.11.0-snapshot"}) and not in_range(sdk_version, {"start": "1.14.0-snapshot"})
    ) or (
        # any post 1.14.0 platform supports any pre 1.16 SDK
        in_range(platform_version, {"start": "1.14.0-snapshot"}) and not in_range(sdk_version, {"start": "1.16.0-snapshot"})
    ) or (
        # any post 1.15.0 platform supports any pre 2.6 SDK
        in_range(platform_version, {"start": "1.15.0-snapshot"}) and not in_range(sdk_version, {"start": "2.5.0-snapshot"})
    ) or (
        # any post 2.5.0 platform supports any SDK
        in_range(platform_version, {"start": "2.5.0-snapshot"})
    )

def sdk_platform_test(sdk_version, platform_version):
    # SDK components
    daml_assistant = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    dar_files = "@daml-sdk-{sdk_version}//:dar-files".format(
        sdk_version = sdk_version,
    )

    # Platform components
    canton_sandbox = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )
    canton_sandbox_args = ["sandbox", "--canton-port-file", "__PORTFILE__"]

    json_api = "@daml-sdk-{platform_version}//:daml".format(
        platform_version = platform_version,
    )

    # daml-ledger test-cases
    name = "daml-ledger-{sdk_version}-platform-{platform_version}".format(
        sdk_version = version_to_name(sdk_version),
        platform_version = version_to_name(platform_version),
    )

    # Canton does not support LF<=1.8 found in earlier versions
    if versions.is_at_least("1.16.0", sdk_version):
        daml_ledger_test(
            name = name,
            sdk_version = sdk_version,
            daml = daml_assistant,
            sandbox = canton_sandbox,
            sandbox_args = canton_sandbox_args,
            size = "large",
            # We see timeouts here fairly regularly so we
            # increase the number of CPUs.
            tags = ["cpu:2"] + extra_tags(sdk_version, platform_version),
        )

    # For now, we only cover the Daml Hub usecase where
    # sandbox and the JSON API come from the same SDK.
    # However, the test setup is flexible enough, that we
    # can control them individually.

    # We allocate parties via @daml/ledger which only supports this since SDK 1.8.0
    if versions.is_at_least("2.0.1", sdk_version):
        create_daml_app_test(
            name = "create-daml-app-{sdk_version}-platform-{platform_version}".format(sdk_version = version_to_name(sdk_version), platform_version = version_to_name(platform_version)),
            daml = daml_assistant,
            sandbox = canton_sandbox,
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
            tags = extra_tags(sdk_version, platform_version) + ["exclusive", "dont-run-on-darwin"],
        )
