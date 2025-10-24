# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
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

    server = daml_runner
    server_args = ["sandbox", "--canton-port-file", "_port_file"]
    server_files = []
    server_files_prefix = ""

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

if [ {wait_for_port_file} -eq 1 ]; then
    echo "Waiting for Canton port file to appear..."
    timeout=60
    while [ ! -e _port_file ]; do
        if [ "$$timeout" = 0 ]; then
            echo "Timed out waiting for Canton startup" >&2
            exit 1
        fi
        echo "waiting $${{timeout}}s"
        sleep 1
        timeout=$$((timeout - 1))
    done
    echo "Canton port file appeared."
fi

JQ=$$(canonicalize_rlocation $(rootpath {jq}))
GRPCURL=$$(canonicalize_rlocation $(rootpath {grpcurl}))

# Validate that the Admin API port is a valid number
ADMIN_PORT=$$($$JQ '.sandbox.adminApi' _port_file)
if echo "$$ADMIN_PORT" | $$JQ -e 'type != "number"' >/dev/null; then
  echo "Could not retrieve Admin API port at .sandbox.adminApi in _port_file:" >&2
  cat _port_file >&2
  echo
  exit 1
fi

if [ {wait_for_synchronizer} -eq 1 ]; then
    echo "Waiting for synchronizer to connect by listing connected synchronizers on Admin API port '$$ADMIN_PORT'..."
    timeout=60
    while ! $$GRPCURL -plaintext -d '{{}}' 0.0.0.0:$$ADMIN_PORT com.digitalasset.canton.admin.participant.v30.SynchronizerConnectivityService/ListConnectedSynchronizers | $$JQ -e '.connectedSynchronizers | length > 0' >/dev/null; do
        if [ "$$timeout" = 0 ]; then
            echo "Timed out waiting for synchronizer to connect." >&2
            exit 1
        fi
        echo "waiting $${{timeout}}s"
        sleep 1
        timeout=$$((timeout - 1))
    done
    echo "Synchronizer connected successfully."
fi

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
EOF
chmod +x $(OUTS)
""".format(
            dar = compiled_dar,
            runner = daml_runner,
            script_name = script_name,
            upload_dar = "1",
            wait_for_port_file = "1",
            wait_for_synchronizer = "1",
            jq = "@jq_nix//:bin/jq",
            grpcurl = "@grpcurl_nix//:bin/grpcurl",
        ),
        tools = [
            compiled_dar,
            daml_runner,
            "@jq_nix//:bin/jq",
            "@grpcurl_nix//:bin/grpcurl",
        ],
    )

    native.sh_binary(
        name = "{}-client".format(name),
        srcs = ["{}-client.sh".format(name)],
        data = [
            compiled_dar,
            daml_runner,
            "@jq_nix//:bin/jq",
            "@grpcurl_nix//:bin/grpcurl",
        ],
    )

    client_server_test(
        name = name,
        client = "{}-client".format(name),
        client_args = [],
        client_files = [],
        data = [
            compiled_dar,
        ],
        runner = "//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = server,
        server_args = server_args,
        server_files = server_files,
        server_files_prefix = server_files_prefix,
        tags = extra_tags(compiler_version, runner_version) + ["exclusive"],
    )

def daml_script_example_test(compiler_version, runner_version):
    if versions.is_at_least("3.0.0", compiler_version):
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
