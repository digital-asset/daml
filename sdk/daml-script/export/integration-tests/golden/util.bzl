# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@build_environment//:configuration.bzl", "ghc_version", "sdk_version")
load(
    "//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load(
    "//bazel_tools/sh:sh.bzl",
    "sh_inline_binary",
)
load("@os_info//:os_info.bzl", "is_windows")

def daml_ledger_export_test(
        name,
        dar,
        script_identifier,
        parties,
        expected_daml,
        expected_args_json,
        expected_daml_yaml,
        dev = False,
        timeout = "short"):
    # Disabled on Windows since postgres gets unhappy in client_server_test.
    if not is_windows:
        server_dev_args = ["-C ledger.engine.allowed-language-versions=daml-lf-dev-mode-unsafe"] if dev else []
        client_name = name + "-client"

        daml_ledger_export_test_client(
            name = client_name,
            expected_daml = expected_daml,
            expected_args_json = expected_args_json,
            expected_daml_yaml = expected_daml_yaml,
        )

#  Commented out - awaiting a port to canton
#        client_server_test(
#            name = name,
#            client = client_name,
#            client_args = [
#                "--target-port=%PORT%",
#                "--script-identifier=%s" % script_identifier,
#                "--party=" + ",".join(parties),
#            ],
#            client_files = ["$(rootpath %s)" % dar],
#            data = [dar],
#            server = "//ledger/sandbox-on-x:app",
#            server_args = [
#                "run",
#                "-C ledger.participants.default.api-server.port=0",
#                "-C ledger.participants.default.api-server.port-file=%PORT_FILE%",
#            ] + server_dev_args,
#            timeout = timeout,
#        )

# Generate the Daml ledger export and compare to the expected files. This is
# used both for golden tests on ledger exports and to make sure that the
# documentation stays up-to-date.
#
# Normalizes the expected output by removing the copyright header and any
# documentation import markers and normalizes the actual output by adding a
# newline to the last line if missing.
#
# Normalizes the data-dependencies by replacing the SDK version, package-id
# hashes with a placeholder, and Windows path separators by Unix separators.
def daml_ledger_export_test_client(
        name,
        expected_daml,
        expected_args_json,
        expected_daml_yaml):
    client = "//daml-script/export/integration-tests/example-export-client"
    cmd = """\
set -euo pipefail

CLIENT=$$(canonicalize_rlocation $$(get_exe $(rootpaths {client})))

EXPECTED_EXPORT=$$(canonicalize_rlocation $(rootpath {expected_daml}))
EXPECTED_ARGS=$$(canonicalize_rlocation $(rootpath {expected_args_json}))
EXPECTED_YAML=$$(canonicalize_rlocation $(rootpath {expected_daml_yaml}))

ACTUAL_DIR=$$(mktemp -d)
ACTUAL_EXPORT="$$ACTUAL_DIR/out.daml"
ACTUAL_ARGS="$$ACTUAL_DIR/args.json"
ACTUAL_YAML="$$ACTUAL_DIR/daml.yaml"

$$CLIENT $$@ --output="$$ACTUAL_EXPORT $$ACTUAL_ARGS $$ACTUAL_YAML"

# Normalize the expected file by removing the copyright header and any documentation import markers.
# Normalize the actual output by adding a newline to the last line if missing.
$(POSIX_DIFF) -Naur --strip-trailing-cr <($(POSIX_SED) '1,3d;/^-- EXPORT/d' $$EXPECTED_EXPORT) <($(POSIX_SED) '$$a\\' $$ACTUAL_EXPORT) || {{
  echo "$$EXPECTED_EXPORT did not match $$ACTUAL_EXPORT"
  exit 1
}}
$(POSIX_DIFF) -Naur --strip-trailing-cr $$EXPECTED_ARGS <($(POSIX_SED) '$$a\\' $$ACTUAL_ARGS) || {{
  echo "$$EXPECTED_ARGS did not match $$ACTUAL_ARGS"
  exit 1
}}
# Normalize the expected file by removing the copyright header and any documentation import markers.
# Normalize the data-dependencies by replacing the SDK version, package-id hashes with a placeholder, and Windows path separators by Unix separators.
$(POSIX_DIFF) -Naur --strip-trailing-cr <($(POSIX_SED) '1,3d;s/[0-9a-f]\\{{64\\}}/HASH/;s/daml-\\(script\\|stdlib\\)-0\\.0\\.0/daml-\\1-{ghc_version}/;s/sdk-version: 0\\.0\\.0/sdk-version: {sdk_version}/' $$EXPECTED_YAML) <($(POSIX_SED) 's/[0-9a-f]\\{{64\\}}/HASH/;s,\\\\,/,g;$$a\\' $$ACTUAL_YAML) || {{
  echo "$$EXPECTED_YAML did not match $$ACTUAL_YAML"
  exit 1
}}
""".format(
        client = client,
        expected_daml = expected_daml,
        expected_args_json = expected_args_json,
        expected_daml_yaml = expected_daml_yaml,
        ghc_version = ghc_version,
        sdk_version = sdk_version,
    )
    sh_inline_binary(
        name = name,
        cmd = cmd,
        data = [
            client,
            expected_daml,
            expected_args_json,
            expected_daml_yaml,
        ],
        toolchains = ["@rules_sh//sh/posix:make_variables"],
    )
