# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)
load("//bazel_tools:versions.bzl", "version_to_name", "versions")
load("//bazel_tools:testing.bzl", "extra_tags")

def daml_script_example_dar(sdk_version, override_daml_script = None):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    ext = "" if override_daml_script == None else "-with-script-override"
    native.genrule(
        name = "script-example-dar-{sdk_version}{ext}".format(
            sdk_version = version_to_name(sdk_version),
            ext = ext,
        ),
        srcs = ["//bazel_tools/daml_script:example/src/ScriptExample.daml"] + ([] if override_daml_script == None else [override_daml_script]),
        outs = ["script-example-{sdk_version}{ext}.dar".format(
            sdk_version = version_to_name(sdk_version),
            ext = ext,
        )],
        tools = [daml, "@tar_dev_env//:tar"],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
DAML_CACHE=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
{extract_script}
mkdir -p $$TMP_DIR/src
cp -L $(location //bazel_tools/daml_script:example/src/ScriptExample.daml) $$TMP_DIR/src/
cat <<EOF >$$TMP_DIR/daml.yaml
{component_spec}
name: script-example
source: src
version: 0.0.1
dependencies:
  - daml-prim
  - daml-stdlib
  - daml-script
build-options:
  - -Wno-deprecated-exceptions
  - -Wno-upgrade-interfaces
  - -Wno-template-interface-depends-on-daml-script
EOF
# TODO(dpm#12) Dpm doesn't support building a package via any kind of `--package-root` flag, so we must CD for now. Revert back to a flag once dpm supports this
PREV_PWD=$$PWD
cd $$TMP_DIR
DAML_CACHE=$$DAML_CACHE $$PREV_PWD/$(location {daml}) build -o $$PREV_PWD/$(OUTS)
""".format(
            daml = daml,
            extract_script =
                "" if override_daml_script == None else "mkdir -p $$TMP_DIR/script-component-extracted && $(location @tar_dev_env//:tar) -xf $(location {script}) -C $$TMP_DIR/script-component-extracted".format(script = override_daml_script),
            component_spec =
                "sdk-version: {sdk_version}".format(sdk_version = sdk_version) if override_daml_script == None else """\
components:
  - name: daml-script
    path: ./script-component-extracted
  - damlc:{sdk_version}
""".format(sdk_version = sdk_version),
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
    server_args = ["sandbox", "--debug", "--canton-port-file", "_port_file"]
    server_files = ["$(rootpath {})".format(compiled_dar)]
    server_files_prefix = "--dar="

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

timeout=60
while [ ! -e _port_file ]; do
    if [ "$$timeout" = 0 ]; then
        echo "Timed out waiting for Canton startup" >&2
        exit 1
    fi
    sleep 1
    timeout=$$((timeout - 1))
done

DAML_SDK_VERSION={runner_version} $$runner script \\
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
            runner_version = runner_version,
            script_name = script_name,
        ),
        tools = [
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
        script_name = "ScriptExample:main",
    )
