# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_linux")
load("//bazel_tools:testing.bzl", "sdk_platform_test")
load(
    "//bazel_tools:version.bzl",
    "matrix_test",
    "version_flag",
    "versioned_binary",
    "versioned_file",
)

sdk_versions = [
    "1.0.0",
    "1.0.1-snapshot.20200417.3908.1.722bac90",
    "1.1.0-snapshot.20200422.3991.0.6391ee9f",
]

platform_versions = [
    "1.0.0",
    "1.0.1-snapshot.20200417.3908.1.722bac90",
    "1.1.0-snapshot.20200422.3991.0.6391ee9f"
]

[
    sh_binary(
        name = "sandbox-with-postgres-{}".format(version),
        srcs = ["@//bazel_tools:sandbox-with-postgres.sh"],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = ["@//bazel_tools/client_server:with-postgres", "@daml-sdk-{}//:daml".format(version)],
    ) for version in platform_versions
]

version_flag(
    name = "sdk_version",
    build_setting_default = sdk_versions[0],
)

version_flag(
    name = "platform_version",
    build_setting_default = platform_versions[0],
)

versioned_file(
    name = "dar-files",
    flag = ":sdk_version",
    versions = {
        version: "@daml-sdk-%s//:dar-files" % version
        for version in sdk_versions
    },
)

versioned_binary(
    name = "ledger-api-test-tool",
    flag = ":sdk_version",
    versions = {
        version: "@daml-sdk-%s//:ledger-api-test-tool" % version
        for version in sdk_versions
    },
)

versioned_binary(
    name = "sandbox",
    flag = ":platform_version",
    versions = {
        version: "@daml-sdk-%s//:daml" % version
        for version in platform_versions
    },
)

client_server_test(
    name = "ledger-api-test-tool-test",
    client = ":ledger-api-test-tool",
    client_args = [
        "localhost:6865",
        "--open-world",
        "--exclude=ClosedWorldIT",
    ],
    data = [":dar-files"],
    runner = "@//bazel_tools/client_server:runner",
    runner_args = ["6865"],
    server = ":sandbox",
    server_args = ["sandbox"],
    server_files = ["$(rootpaths :dar-files)"],
    tags = [
        "exclusive",
        "manual",
    ],
)

client_server_test(
    name = "ledger-api-test-tool-postgres-test",
    client = ":ledger-api-test-tool",
    client_args = [
        "localhost:6865",
        "--open-world",
        "--exclude=ClosedWorldIT",
    ],
    data = [":dar-files"],
    runner = "@//bazel_tools/client_server:runner",
    runner_args = ["6865"],
    server = ":sandbox",
    server_args = [],
    server_files = ["$(rootpaths :dar-files)"],
    tags = [
        "exclusive",
        "manual",
    ],
)

[
    (
        matrix_test(
            name = "ledger-api-test-tool-{sdk_version}-platform-{platform_version}".format(
                sdk_version = sdk_version,
                platform_version = platform_version,
            ),
            test = "ledger-api-test-tool-test",
            tags = [
                "exclusive",
                "sdk_version=%s" % sdk_version,
                "platform_version=%s" % platform_version,
            ],
        ),
        matrix_test(
           name = "ledger-api-test-tool-{sdk_version}-platform-{platform_version}-postgresql".format(
               sdk_version = sdk_version,
               platform_version = platform_version,
           ),
           test = "ledger-api-test-tool-test",
           tags = [
               "exclusive",
               "sdk_version=%s" % sdk_version,
               "platform_version=%s" % platform_version,
           ],
       )
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
] if is_linux else None

[
    sdk_platform_test(
        sdk_version = sdk_version,
        platform_version = platform_version,
    )
    for sdk_version in sdk_versions
    for platform_version in platform_versions
] if is_linux else None
