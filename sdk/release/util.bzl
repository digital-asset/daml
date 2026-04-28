# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//daml-lf:daml-lf.bzl", "SUPPORTED_PROTO_STABLE_LF_VERSIONS")
load("@build_environment//:configuration.bzl", "sdk_version")
load("@os_info//:os_info.bzl", "is_windows")

dpm_inputs = {
    "damlc": "//compiler/damlc:damlc-oci.tar.gz",
    "daml-script": "//daml-script/runner:daml-script-oci.tar.gz",
    "codegen": "//language-support/codegen-main:codegen-oci.tar.gz",
    "daml-new": "//daml-assistant/daml-new:daml-new-oci.tar.gz",
    "upgrade-check": "//daml-assistant/upgrade-check-main:upgrade-check-oci.tar.gz",
    "canton-open-source": "//canton:canton-community-oci.tar.gz",
}

not_windows_wrapper_script = """
cat > "$$DIR/bin/dpm" << EOF
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=\\$$( cd -- "\\$$( dirname -- "\\$${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export DPM_HOME=\\$$(dirname -- \\$$SCRIPT_DIR)
export HOME=\\$$DPM_HOME
\\$$DPM_HOME/cache/components/dpm/$$DPM_VERSION/dpm \\$$@
EOF
chmod +x $$DIR/bin/dpm
"""

windows_wrapper_script = """
cat > "$$DIR/bin/dpm.cmd" << EOF
@echo off
FOR %%A IN ("%~dp0.") DO SET DPM_HOME=%%~dpA
IF %DPM_HOME:~-1%==\\ SET DPM_HOME=%DPM_HOME:~0,-1%

set HOME=%DPM_HOME%
set APPDATA=%DPM_HOME%

set DPM_VERSION=$$DPM_VERSION

"%DPM_HOME%\\cache\\components\\dpm\\%DPM_VERSION%\\dpm.exe" %*
EOF
chmod +x $$DIR/bin/dpm.cmd
"""

def dpm_sdk_tarball(name, version):
    native.genrule(
        name = name,
        srcs = [dpm_inputs.get(name) for name in dpm_inputs.keys()],
        outs = ["{}.tar.gz".format(name)],
        tools = ["//bazel_tools/sh:mktgz", "@dpm_binary//:dpm"],
        cmd = """
          TMP=$$(mktemp -d)
          trap "rm -rf $$TMP" EXIT
          mkdir -p $$TMP/{name}
          DIR=$$TMP/{name}

          DPM_VERSION=$$(HOME=. DPM_HOME=. USERPROFILE=. $(location @dpm_binary//:dpm) -v | head -n 1 | sed -e "s/^version: //")
          
          # Need to build the installation directory, can't build the oci thing, its too hard
          mkdir $$DIR/bin
          {wrapper_script}

          cat > "$$DIR/dpm-config.yaml" << EOF
edition: open-source
registry: europe-docker.pkg.dev/da-images/public-unstable
EOF

          mkdir -p $$DIR/cache/components
          {unpack_inputs}

          mkdir -p $$DIR/cache/components/dpm/$$DPM_VERSION
          cp $(location @dpm_binary//:dpm) $$DIR/cache/components/dpm/$$DPM_VERSION/dpm{exe}

          mkdir -p $$DIR/cache/sdk/open-source

          cat > "$$DIR/cache/sdk/open-source/{version}.yaml" << EOF
apiVersion: digitalasset.com/v1
kind: SdkManifest
spec:
  components:
    {component_versions}
  assistant:
    version: $$DPM_VERSION
  version: {version}
  edition: open-source
EOF

          MKTGZ=$$PWD/$(execpath //bazel_tools/sh:mktgz)
          OUT_PATH=$$PWD/$@
          cd $$TMP

          $$MKTGZ $$OUT_PATH {name}
        """.format(
            name = name,
            version = version,
            unpack_inputs =
                "\n          ".join(
                    [
                        "mkdir -p $$DIR/cache/components/{name}/{version} && tar -xzf $(location {path}) -C $$DIR/cache/components/{name}/{version}"
                            .format(name = name, path = path, version = version)
                        for name, path in dpm_inputs.items()
                    ],
                ),
            component_versions =
                "\n    ".join(["{name}:\n      version: {version}".format(name = name, version = version) for name in dpm_inputs.keys()]),
            wrapper_script = windows_wrapper_script if is_windows else not_windows_wrapper_script,
            exe = ".exe" if is_windows else "",
        ),
        visibility = ["//visibility:public"],
    )
