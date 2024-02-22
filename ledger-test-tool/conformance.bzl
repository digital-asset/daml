# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("@os_info//:os_info.bzl", "is_intel", "is_windows")
load("@scala_version//:index.bzl", "scala_major_version")
load("//daml-lf/language:daml-lf.bzl", "lf_version_configuration", "lf_version_is_dev", "lf_versions_aggregate")

def conformance_test(
        name,
        server,
        server_args = [],
        extra_data = [],
        ports = [6865],
        test_tool_args = [],
        tags = [],
        runner = "@//bazel_tools/client_server/runner_with_port_check",
        extra_runner_args = [],
        lf_versions = ["default"],
        dev_mod_flag = "--daml-lf-dev-mode-unsafe",
        preview_mod_flag = "--early-access",
        flaky = False,
        hocon = False,
        server_hocon_config = None):
    for lf_version in lf_versions_aggregate(lf_versions):
        daml_lf_dev_mode_args = ["-C ledger.engine.allowed-language-versions=daml-lf-dev-mode-unsafe"] if hocon else [dev_mod_flag]
        daml_lf_preview_mode_args = ["-C ledger.engine.allowed-language-versions=early-access"] if hocon else [preview_mod_flag]
        extra_server_args = daml_lf_preview_mode_args if lf_version == lf_version_configuration.get("preview") else daml_lf_dev_mode_args if lf_version_is_dev(lf_version) else []
        if not is_windows:
            test_name = "-".join([name, lf_version])
            hocon_conf_file_name = test_name + ".conf"
            if server_hocon_config:
                generate_conf("generate-" + test_name, hocon_conf_file_name, content = server_hocon_config, data = extra_data)
            hocon_server_args = ["-c $(rootpath :" + hocon_conf_file_name + ")"] if server_hocon_config else []
            hocon_data = [":" + hocon_conf_file_name] if server_hocon_config else []
            client_server_test(
                name = test_name,
                runner = runner,
                runner_args = ["%s" % port for port in ports] + extra_runner_args,
                timeout = "long",
                client = "//ledger-test-tool/tool:tool-%s" % lf_version,
                client_args = test_tool_args + ["localhost:%s" % port for port in ports],
                data = extra_data + hocon_data,
                server = server,
                server_args = server_args + extra_server_args + hocon_server_args,
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

def generate_conf(name, conf_file_name, content, data = []):
    native.genrule(
        name = name,
        srcs = data,
        outs = [conf_file_name],
        cmd = """
set -eou pipefail
cat << 'EOF' > $@
{content}
EOF
        """.format(content = content),
        visibility = ["//visibility:public"],
    )

# versions for which we build a test-tool
testtool_lf_versions = (["1.8", "1.14"] if is_intel else []) + ["1.15", "1.dev"]
