# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def daml_script_dar(sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    native.genrule(
        name = "script-example-dar-{sdk_version}".format(
            sdk_version = sdk_version,
        ),
        srcs = ["//bazel_tools/daml_script:example/src/ScriptExample.daml"],
        outs = ["script-example-{sdk_version}.dar".format(
            sdk_version = sdk_version,
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
