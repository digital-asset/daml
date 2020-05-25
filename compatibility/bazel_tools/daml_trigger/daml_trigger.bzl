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
        srcs = ["//bazel_tools/daml_trigger:example/src/CopyTrigger.daml"],
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
cat <<EOF >$$TMP_DIR/daml.yaml
sdk-version: {sdk_version}
name: trigger-example
source: src
parties:
  - Alice
  - Bob
version: 0.0.1
dependencies:
  - daml-prim
  - daml-stdlib
  - daml-trigger
sandbox-options:
  - --wall-clock-time
EOF
$(location {daml}) build --project-root=$$TMP_DIR -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            sdk_version = sdk_version,
        ),
    )
