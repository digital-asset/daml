# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("//bazel_tools:versions.bzl", "version_to_name")

def daml_script_dar(sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    native.genrule(
        name = "script-example-dar-{sdk_version}".format(
            sdk_version = version_to_name(sdk_version),
        ),
        srcs = ["//bazel_tools/daml_script:example/src/ScriptExample.daml"],
        outs = ["script-example-{sdk_version}.dar".format(
            sdk_version = version_to_name(sdk_version),
        )],
        tools = [daml],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
mkdir -p $$TMP_DIR/src
cp -L $(location //bazel_tools/daml_script:example/src/ScriptExample.daml) $$TMP_DIR/src/
cat <<EOF >$$TMP_DIR/daml.yaml
sdk-version: {sdk_version}
name: script-example
source: src
parties:
  - Alice
  - Bob
  - Bank
version: 0.0.1
dependencies:
  - daml-prim
  - daml-stdlib
  - daml-script
sandbox-options:
  - --wall-clock-time
EOF
$(location {daml}) build --project-root=$$TMP_DIR -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            sdk_version = sdk_version,
        ),
    )

def daml_script_test(compiler_version, runner_version):
    compiled_dar = "//:script-example-dar-{version}".format(
        version = version_to_name(compiler_version),
    )
    daml_runner = "@daml-sdk-{version}//:daml".format(
        version = runner_version,
    )
    client_server_test(
        name = "daml-script-test-compiler-{compiler_version}-runner-{runner_version}".format(
            compiler_version = version_to_name(compiler_version),
            runner_version = version_to_name(runner_version),
        ),
        client = daml_runner,
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
            "$(rootpath {})".format(compiled_dar),
        ],
        data = [
            compiled_dar,
        ],
        runner = "//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = daml_runner,
        server_args = ["sandbox"],
        server_files = [
            "$(rootpath {})".format(compiled_dar),
        ],
        tags = ["exclusive"],
    )
