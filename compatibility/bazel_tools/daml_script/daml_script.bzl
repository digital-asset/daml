# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("//bazel_tools:versions.bzl", "version_to_name", "versions")

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

    # 1.16.0 is the first SDK version that defaulted to LF 1.14, which is the earliest LF version that Canton supports
    use_canton = versions.is_at_least("2.0.0", runner_version) and versions.is_at_least("1.16.0", compiler_version)
    use_sandbox_on_x = versions.is_at_least("2.0.0", runner_version) and not use_canton

    if use_sandbox_on_x:
        server = "@daml-sdk-{version}//:sandbox-on-x".format(version = runner_version)
        server_args = ["--participant", "participant-id=sandbox,port=6865"]
        server_files = []
        runner_files = ["$(rootpath {})".format(compiled_dar)]
    else:
        server = daml_runner
        server_args = ["sandbox"]
        server_files = ["$(rootpath {})".format(compiled_dar)]
        runner_files = []

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
        runner = "//bazel_tools/client_server/with-upload-dar:runner",
        runner_args = ["--port=6865"],
        runner_files = runner_files,
        runner_files_prefix = "--dar=",
        server = server,
        server_args = server_args,
        server_files = server_files,
        server_files_prefix = "--dar=" if use_canton else "",
        tags = ["exclusive"],
    )
