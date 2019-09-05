# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//bazel_tools/client_server_test:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_windows")

def conformance_test(name, server, server_args = [], extra_data = [], ports = [6865], test_tool_args = []):
    client_server_test(
        name = name,
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
        ],
    ) if not is_windows else None
