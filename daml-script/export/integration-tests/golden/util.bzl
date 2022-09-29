# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@build_environment//:configuration.bzl", "ghc_version", "sdk_version")
load(
    "//bazel_tools/client_server:client_server_build.bzl",
    "client_server_build",
)
load(
    "//bazel_tools/sh:sh.bzl",
    "sh_inline_test",
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
        out_daml,
        out_args_json,
        out_daml_yaml):
    # Disabled on Windows since postgres gets unhappy in client_server_build.
    if not is_windows:
        actual_daml = name + "/" + out_daml
        actual_args_json = name + "/" + out_args_json
        actual_daml_yaml = name + "/" + out_daml_yaml

        client_server_build(
            name = name,
            outs = [
                actual_daml,
                actual_args_json,
                actual_daml_yaml,
            ],
            client = "//daml-script/export/integration-tests/example-export-client",
            client_args = [
                "--target-port=%PORT%",
                "--script-identifier={}".format(script_identifier),
                "--party=" + ",".join(parties),
            ],
            client_files = [dar],
            data = [dar],
            output_env = "EXPORT_OUT",
            server = "//ledger/sandbox-on-x:sandbox-on-x-ephemeral-postgresql",
            server_args = [
                "run-legacy-cli-config --participant=participant-id=example,port=0,port-file=%PORT_FILE%",
            ],
        )

        # Compare the generated Daml ledger export to the expected files.
        # This is used both for golden tests on ledger exports and to
        # make sure that the documentation stays up-to-date.
        #
        # Normalizes the expected output by removing the copyright header and any
        # documentation import markers and normalizes the actual output by adding a
        # newline to the last line if missing.
        #
        # Normalizes the data-dependencies by replacing the SDK version, package-id
        # hashes with a placeholder, and Windows path separators by Unix separators.
        sh_inline_test(
            name = name + "-compare",
            cmd = """\
EXPECTED_EXPORT=$$(canonicalize_rlocation $(rootpath {expected_daml}))
EXPECTED_ARGS=$$(canonicalize_rlocation $(rootpath {expected_args_json}))
EXPECTED_YAML=$$(canonicalize_rlocation $(rootpath {expected_daml_yaml}))
ACTUAL_EXPORT=$$(canonicalize_rlocation $(rootpath :{actual_daml}))
ACTUAL_ARGS=$$(canonicalize_rlocation $(rootpath :{actual_args_json}))
ACTUAL_YAML=$$(canonicalize_rlocation $(rootpath :{actual_daml_yaml}))
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
                expected_daml = expected_daml,
                expected_args_json = expected_args_json,
                expected_daml_yaml = expected_daml_yaml,
                actual_daml = actual_daml,
                actual_args_json = actual_args_json,
                actual_daml_yaml = actual_daml_yaml,
                ghc_version = ghc_version,
                sdk_version = sdk_version,
            ),
            data = [
                expected_daml,
                expected_args_json,
                expected_daml_yaml,
                actual_daml,
                actual_args_json,
                actual_daml_yaml,
            ],
            toolchains = ["@rules_sh//sh/posix:make_variables"],
        )
