# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test_canton_sh",
)
load("//bazel_tools:versions.bzl", "version_to_name", "versions")
load("//bazel_tools:testing.bzl", "extra_tags")

def daml_script_example_dar(sdk_version):
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
        # TODO(#17366): remove explicit target once LF2 is the default
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
build-options: [--target=2.1]
sandbox-options:
  - --wall-clock-time
EOF
$(location {daml}) build --project-root=$$TMP_DIR -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            sdk_version = sdk_version,
        ),
    )

def daml_script_test(
        name,
        compiler_version,
        runner_version,
        compiled_dar,
        script_name):
    daml_runner = "@daml-sdk-{version}//:daml".format(
        version = runner_version,
    )
    client_server_test_canton_sh(
        name = name,
        data = [
            compiled_dar,
            daml_runner,
        ],
        server_files = ["$(rootpath {})".format(compiled_dar)],
        tags = extra_tags(compiler_version, runner_version),
        src = """\
runner=$$(canonicalize_rlocation $(rootpath {runner}))

if [ {upload_dar} -eq 1 ] ; then
  $$runner ledger upload-dar \\
    --host localhost \\
    --port 6865 \\
    $$(canonicalize_rlocation $(rootpath {dar}))
fi

$$runner script \\
  --ledger-host localhost \\
  --ledger-port 6865 \\
  --wall-clock-time \\
  --dar $$(canonicalize_rlocation $(rootpath {dar})) \\
  --script-name {script_name}
""".format(
            dar = compiled_dar,
            runner = daml_runner,
            script_name = script_name,
            upload_dar = "0",
            wait_for_port_file = "1",
        ),
    )

def daml_script_example_test(compiler_version, runner_version):
    daml_script_test(
        name = "daml-script-test-compiler-{compiler_version}-runner-{runner_version}".format(
            compiler_version = version_to_name(compiler_version),
            runner_version = version_to_name(runner_version),
        ),
        compiler_version = compiler_version,
        runner_version = runner_version,
        compiled_dar = "//:script-example-dar-{version}".format(
            version = version_to_name(compiler_version),
        ),
        script_name = "ScriptExample:test",
    )
