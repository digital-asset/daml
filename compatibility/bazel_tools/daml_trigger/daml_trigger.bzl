# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("//bazel_tools:versions.bzl", "version_to_name")

def daml_trigger_dar(sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    native.genrule(
        name = "trigger-example-dar-{sdk_version}".format(
            sdk_version = version_to_name(sdk_version),
        ),
        srcs = [
            "//bazel_tools/daml_trigger:example/src/CopyTrigger.daml",
            "//bazel_tools/daml_trigger:example/src/TestScript.daml",
        ],
        outs = ["trigger-example-{sdk_version}.dar".format(
            sdk_version = version_to_name(sdk_version),
        )],
        tools = [daml],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
mkdir -p $$TMP_DIR/src
cp -L $(location //bazel_tools/daml_trigger:example/src/CopyTrigger.daml) $$TMP_DIR/src/
cp -L $(location //bazel_tools/daml_trigger:example/src/TestScript.daml) $$TMP_DIR/src/
cat <<EOF >$$TMP_DIR/daml.yaml
sdk-version: {sdk_version}
name: trigger-example
source: src
version: 0.0.1
dependencies:
  - daml-prim
  - daml-script
  - daml-stdlib
  - daml-trigger
EOF
$(location {daml}) build --project-root=$$TMP_DIR -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            sdk_version = sdk_version,
        ),
    )

def daml_trigger_test(compiler_version, runner_version):
    compiled_dar = "//:trigger-example-dar-{version}".format(
        version = version_to_name(compiler_version),
    )
    daml_runner = "@daml-sdk-{version}//:daml".format(
        version = runner_version,
    )
    name = "daml-trigger-test-compiler-{compiler_version}-runner-{runner_version}".format(
        compiler_version = version_to_name(compiler_version),
        runner_version = version_to_name(runner_version),
    )
    native.genrule(
        name = "{}-client-sh".format(name),
        outs = ["{}-client.sh".format(name)],
        cmd = """\
cat >$(OUTS) <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
canonicalize_rlocation() {{
  # Note (MK): This is a fun one: Let's say $$TEST_WORKSPACE is "compatibility"
  # and the argument points to a target from an external workspace, e.g.,
  # @daml-sdk-0.0.0//:daml. Then the short path will point to
  # ../daml-sdk-0.0.0/daml. Putting things together we end up with
  # compatibility/../daml-sdk-0.0.0/daml. On Linux and MacOS this works
  # just fine. However, on windows we need to normalize the path
  # or rlocation will fail to find the path in the manifest file.
  rlocation $$(realpath -L -s -m --relative-to=$$PWD $$TEST_WORKSPACE/$$1)
}}
runner=$$(canonicalize_rlocation $(rootpath {runner}))
# Cleanup the trigger runner process but maintain the script runner exit code.
trap 'status=$$?; kill -TERM $$PID; wait $$PID; exit $$status' INT TERM
$$runner trigger \\
  --ledger-host localhost \\
  --ledger-port 6865 \\
  --ledger-party Alice \\
  --wall-clock-time \\
  --dar $$(canonicalize_rlocation $(rootpath {dar})) \\
  --trigger-name CopyTrigger:copyTrigger &
PID=$$!
$$runner script \\
  --ledger-host localhost \\
  --ledger-port 6865 \\
  --wall-clock-time \\
  --dar $$(canonicalize_rlocation $(rootpath {dar})) \\
  --script-name TestScript:test
EOF
chmod +x $(OUTS)
""".format(
            dar = compiled_dar,
            runner = daml_runner,
        ),
        exec_tools = [
            compiled_dar,
            daml_runner,
        ],
    )
    native.sh_binary(
        name = "{}-client".format(name),
        srcs = ["{}-client.sh".format(name)],
        data = [
            compiled_dar,
            daml_runner,
        ],
    )
    client_server_test(
        name = "daml-trigger-test-compiler-{compiler_version}-runner-{runner_version}".format(
            compiler_version = version_to_name(compiler_version),
            runner_version = version_to_name(runner_version),
        ),
        client = "{}-client".format(name),
        client_args = [],
        client_files = [],
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
