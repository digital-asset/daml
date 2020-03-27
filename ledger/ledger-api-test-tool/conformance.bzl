# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_windows")

def conformance_test(
        name,
        server,
        server_args = [],
        extra_data = [],
        ports = [6865],
        test_tool_args = [],
        tags = [],
        runner = "@//bazel_tools/client_server/runner_with_port_check:runner",
        flaky = False):
    client_server_test(
        name = name,
        runner = runner,
        runner_args = ["%s" % port for port in ports],
        timeout = "long",
        client = "//ledger/ledger-api-test-tool",
        client_args = test_tool_args + ["localhost:%s" % port for port in ports],
        data = extra_data + [
            "//ledger/test-common:dar-files",
        ],
        server = server,
        server_args = server_args,
        server_files = [
            "$(rootpaths //ledger/test-common:dar-files)",
        ],
        tags = [
            "dont-run-on-darwin",
            "exclusive",
        ] + tags,
        flaky = flaky,
    ) if not is_windows else None

def server_conformance_test(name, servers, server_args = [], test_tool_args = [], flaky = False):
    for server_name, server in servers.items():
        test_name = "-".join([segment for segment in [name, server_name] if segment])
        conformance_test(
            name = test_name,
            extra_data = server.get("extra_data", []),
            server = server["binary"],
            server_args = server.get("server_args", []) + server_args,
            test_tool_args = server.get("test_tool_args", []) + test_tool_args,
            flaky = flaky,
        )
