# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_windows")
load("//bazel_tools:testing.bzl", "sdk_platform_test")
load("//bazel_tools/daml_script:daml_script.bzl", "daml_script_dar")

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

client_server_test(
    name = "daml-script",
    client = "@daml-sdk-1.0.0//:daml",
    client_args = [
        "script",
        "--ledger-host",
        "localhost",
        "--ledger-port",
        "6865",
        "--wall-clock-time",
        "--script-name",
        "ScriptExample:test",
        "--dar",
    ],
    client_files = [
        "$(rootpath //:script-example-dar-1.0.0)",
    ],
    data = [
        "//:script-example-dar-1.0.0",
    ],
    runner = "//bazel_tools/client_server:runner",
    runner_args = ["6865"],
    server = "@daml-sdk-1.0.0//:daml",
    server_args = ["sandbox"],
    server_files = ["$(rootpath //:script-example-dar-1.0.0)"],
    tags = ["exclusive"],
)

test_suite(
    name = "head-quick",
    tags = ["head-quick"],
)
