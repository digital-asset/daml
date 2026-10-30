# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools/client_server:client_server_test.bzl", "client_server_test")
load("//compatibility/bazel_tools:sdks.bzl", "COMPAT_TAGS", "daml_sdk")
load("//compatibility/bazel_tools:testing.bzl", "extra_tags")
load("//compatibility/bazel_tools:versions.bzl", "version_to_name")

def daml_script_example_dar(sdk_version, override_daml_script = None):
    daml = daml_sdk(sdk_version)
    suffix = "" if override_daml_script == None else "-with-script-override"
    native.genrule(
        name = "script-example-dar-{sdk_version}{suffix}".format(
            sdk_version = version_to_name(sdk_version),
            suffix = suffix,
        ),
        srcs = ["//compatibility/bazel_tools/daml_script:example/src/ScriptExample.daml"] + ([] if override_daml_script == None else [override_daml_script]),
        outs = ["script-example-{sdk_version}{suffix}.dar".format(
            sdk_version = version_to_name(sdk_version),
            suffix = suffix,
        )],
        tools = [daml, "@bsd_tar_toolchains//:resolved_toolchain"],
        toolchains = ["@bsd_tar_toolchains//:resolved_toolchain"],
        tags = COMPAT_TAGS,
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
DAML_CACHE=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
{extract_script}
mkdir -p $$TMP_DIR/src
cp -L $(location //compatibility/bazel_tools/daml_script:example/src/ScriptExample.daml) $$TMP_DIR/src/
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
                "" if override_daml_script == None else "mkdir -p $$TMP_DIR/script-component-extracted && $(BSDTAR_BIN) -xf $(location {script}) -C $$TMP_DIR/script-component-extracted".format(script = override_daml_script),
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
    daml_runner = daml_sdk(runner_version)

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
runner=$$(rlocation $(rlocationpath {runner}))
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
  --dar $$(rlocation $(rlocationpath {dar})) \\
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
        tags = COMPAT_TAGS,
    )
    native.sh_binary(
        name = "{}-client".format(name),
        srcs = ["{}-client.sh".format(name)],
        data = [
            compiled_dar,
            daml_runner,
        ],
        tags = COMPAT_TAGS,
    )

    client_server_test(
        name = name,
        client = "{}-client".format(name),
        client_args = [],
        client_files = [],
        data = [
            compiled_dar,
        ],
        runner = "//compatibility/bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = server,
        server_args = server_args,
        server_files = server_files,
        server_files_prefix = server_files_prefix,
        tags = COMPAT_TAGS + extra_tags(compiler_version, runner_version) + ["exclusive"],
    )

def daml_script_example_test(compiler_version, runner_version):
    daml_script_test(
        name = "daml-script-test-compiler-{compiler_version}-runner-{runner_version}".format(
            compiler_version = version_to_name(compiler_version),
            runner_version = version_to_name(runner_version),
        ),
        compiler_version = compiler_version,
        runner_version = runner_version,
        compiled_dar = "//compatibility:script-example-dar-{version}".format(
            version = version_to_name(compiler_version),
        ),
        script_name = "ScriptExample:main",
    )
