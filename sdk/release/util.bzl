# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//daml-lf:daml-lf.bzl", "SUPPORTED_PROTO_STABLE_LF_VERSIONS")
load("@build_environment//:configuration.bzl", "sdk_version")
load("@os_info//:os_info.bzl", "is_windows")

inputs = {
    "sdk_config": ":sdk-config.yaml.tmpl",
    "install_sh": ":install.sh",
    "install_bat": ":install.bat",
    "java_codegen_logback": "//language-support/java/codegen:src/main/resources/logback.xml",
    "daml_script_logback": "//daml-script/runner:src/main/resources/logback.xml",
    "NOTICES": "//:NOTICES",
    "daml_dist": "//daml-assistant:daml-dist",
    "daml_helper_dist": "//daml-assistant/daml-helper:daml-helper-dist",
    "damlc_dist": "//compiler/damlc:damlc-dist",
    "daml_extension": "//compiler/daml-extension:vsix",
    "daml_extension_stylesheet": "//compiler/daml-extension:webview-stylesheet.css",
    "templates": "//templates:templates-tarball.tar.gz",
    "script_dars": "//daml-script/daml:daml-script-dars",
    "canton": "//canton:community_app_deploy.jar",
    "sdk_deploy_jar": {
        "ce": "//daml-assistant/daml-sdk:sdk_distribute.jar",
        "ee": "//daml-assistant/daml-sdk:sdk_ee_distribute.jar",
    },
    "license": ":ee-license.txt",
}

def input_target(config, name):
    targets = inputs.get(name)
    if type(targets) == "string":
        return targets
    else:
        return targets.get(config)

def sdk_tarball(name, version, config):
    kwargs = {name: input_target(config, name) for name in inputs.keys()}
    native.genrule(
        name = name,
        srcs = [input_target(config, name) for name in inputs.keys()],
        outs = ["{}.tar.gz".format(name)],
        tools = ["//bazel_tools/sh:mktgz"],
        cmd = """
          # damlc
          VERSION={version}
          DIR=$$(mktemp -d)
          trap "rm -rf $$DIR" EXIT
          OUT=$$DIR/sdk-$$VERSION
          mkdir -p $$OUT

          if [ "{config}" = "ee" ]; then
            cp $(location {license}) $$OUT/LICENSE.txt
          fi

          cp $(location {NOTICES}) $$OUT/NOTICES

          cp $(location {install_sh}) $$OUT/install.sh
          cp $(location {install_bat}) $$OUT/install.bat

          cp $(location {sdk_config}) $$OUT/sdk-config.yaml
          sed -i "s/__VERSION__/$$VERSION/" $$OUT/sdk-config.yaml

          mkdir -p $$OUT/daml
          tar xf $(location {daml_dist}) --strip-components=1 -C $$OUT/daml

          mkdir -p $$OUT/damlc
          tar xf $(location {damlc_dist}) --strip-components=1 -C $$OUT/damlc

          mkdir -p $$OUT/daml-libs
          cp -t $$OUT/daml-libs $(locations {script_dars})

          mkdir -p $$OUT/daml-helper
          tar xf $(location {daml_helper_dist}) --strip-components=1 -C $$OUT/daml-helper

          mkdir -p $$OUT/studio
          cp $(location {daml_extension}) $$OUT/studio/daml-bundled.vsix
          cp $(location {daml_extension_stylesheet}) $$OUT/studio/webview-stylesheet.css

          mkdir -p $$OUT/canton
          cp $(location {canton}) $$OUT/canton/canton.jar

          mkdir -p $$OUT/templates
          tar xf $(location {templates}) --strip-components=1 -C $$OUT/templates

          mkdir -p $$OUT/daml-sdk
          cp $(location {sdk_deploy_jar}) $$OUT/daml-sdk/daml-sdk.jar
          cp -L $(location {java_codegen_logback}) $$OUT/daml-sdk/codegen-logback.xml
          cp -L $(location {daml_script_logback}) $$OUT/daml-sdk/script-logback.xml

          MKTGZ=$$PWD/$(execpath //bazel_tools/sh:mktgz)
          OUT_PATH=$$PWD/$@
          cd $$DIR

          $$MKTGZ $$OUT_PATH $$(basename $$OUT)
        """.format(
            version = version,
            config = config,
            **kwargs
        ),
        visibility = ["//visibility:public"],
    )

dpm_inputs = {
    "damlc": "//compiler/damlc:damlc-oci.tar.gz",
    "daml-script": "//daml-script/runner:daml-script-oci.tar.gz",
    "codegen": "//language-support/codegen-main:codegen-oci.tar.gz",
    "daml-new": "//daml-assistant/daml-helper:daml-new-oci.tar.gz",
    "upgrade-check": "//daml-assistant/upgrade-check-main:upgrade-check-oci.tar.gz",
    "canton-enterprise": "//canton:canton-community-oci.tar.gz",
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
