# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_windows")
load("@scala_version//:index.bzl", "scala_major_version")
load("//daml-lf/language:daml-lf.bzl", "lf_version_configuration", "lf_versions_aggregate")

def conformance_test(
        name,
        server,
        server_args = [],
        extra_data = [],
        ports = [6865],
        test_tool_args = [],
        tags = [],
        runner = "@//bazel_tools/client_server/runner_with_port_check",
        lf_versions = ["default"],
        flaky = False):
    for lf_version in lf_versions_aggregate(lf_versions):
        extra_server_args = ["--daml-lf-dev-mode-unsafe"] if lf_version == lf_version_configuration.get("preview") or lf_version == lf_version_configuration.get("dev") else []
        if not is_windows:
            test_name = "-".join([name, lf_version])
            client_server_test(
                name = test_name,
                runner = runner,
                runner_args = ["%s" % port for port in ports],
                timeout = "long",
                client = "//ledger/ledger-api-test-tool:ledger-api-test-tool-%s" % lf_version,
                client_args = test_tool_args + ["localhost:%s" % port for port in ports],
                data = extra_data,
                server = server,
                server_args = server_args + extra_server_args,
                tags = [
                    "dont-run-on-darwin",
                    "exclusive",
                ] + tags,
                flaky = flaky,
            )
            if lf_version == lf_version_configuration.get("default"):
                native.test_suite(
                    name = name,
                    tests = [test_name],
                    tags = tags,
                )

def server_conformance_test(name, servers, server_args = [], test_tool_args = [], flaky = False, lf_versions = ["default"]):
    for server_name, server in servers.items():
        test_name = "-".join([name, server_name])
        conformance_test(
            name = test_name,
            extra_data = server.get("extra_data", []),
            server = server["binary"],
            server_args = server.get("server_args", []) + server_args,
            test_tool_args = server.get("test_tool_args", []) + test_tool_args,
            lf_versions = lf_versions,
            flaky = flaky,
        )
