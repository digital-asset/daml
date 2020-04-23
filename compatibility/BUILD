# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_linux")

# TODO Abstract over this.
client_server_test(
    name = "ledger-api-test-tool-1.0.1-snapshot.20200417.3908.1.722bac90-platform-1.0.0",
    client = "@daml-sdk-1.0.1-snapshot.20200417.3908.1.722bac90//:ledger-api-test-tool",
    client_args = [
        "localhost:6865",
        "--open-world",
        "--exclude=ClosedWorldIT",
    ],
    data = ["@daml-sdk-1.0.1-snapshot.20200417.3908.1.722bac90//:dar-files"],
    runner = "@//bazel_tools/client_server:runner",
    runner_args = ["6865"],
    server = "@daml-sdk-1.0.0//:daml",
    server_args = ["sandbox"],
    server_files = ["$(rootpaths @daml-sdk-1.0.1-snapshot.20200417.3908.1.722bac90//:dar-files)"],
    tags = ["exclusive"],
) if is_linux else None
