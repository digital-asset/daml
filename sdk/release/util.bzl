# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//daml-lf/language:daml-lf.bzl", "SUPPORTED_PROTO_STABLE_LF_VERSIONS")
load("@build_environment//:configuration.bzl", "sdk_version")

inputs = {
    "sdk_config": ":sdk-config.yaml.tmpl",
    "install_sh": ":install.sh",
    "install_bat": ":install.bat",
    "navigator_logback": "//navigator/backend:src/main/resources/logback.xml",
    "http_json_logback": "//ledger-service/http-json:src/main/resources/logback.xml",
    "oauth2_middleware_logback": "//triggers/service/auth:release/oauth2-middleware-logback.xml",
    "trigger_service_logback": "//triggers/service:release/trigger-service-logback.xml",
    "trigger_logback": "//triggers/runner:src/main/resources/logback.xml",
    "java_codegen_logback": "//language-support/java/codegen:src/main/resources/logback.xml",
    "daml_script_logback": "//daml-script/runner:src/main/resources/logback.xml",
    "export_logback": "//daml-script/export:src/main/resources/logback.xml",
    "NOTICES": "//:NOTICES",
    "daml_dist": "//daml-assistant:daml-dist",
    "daml_helper_dist": "//daml-assistant/daml-helper:daml-helper-dist",
    "damlc_dist": "//compiler/damlc:damlc-dist",
    "daml_extension": "//compiler/daml-extension:vsix",
    "daml_extension_stylesheet": "//compiler/daml-extension:webview-stylesheet.css",
    "daml2js_dist": "//language-support/ts/codegen:daml2js-dist",
    "templates": "//templates:templates-tarball.tar.gz",
    "trigger_dars": "//triggers/daml:daml-trigger-dars",
    "script_dars": "//daml-script/daml:daml-script-dars",
    "script3_dars": "//daml-script/daml3:daml3-script-dars",
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
          cp -t $$OUT/daml-libs $(locations {trigger_dars})
          cp -t $$OUT/daml-libs $(locations {script_dars})
          cp -t $$OUT/daml-libs $(locations {script3_dars})

          mkdir -p $$OUT/daml-helper
          tar xf $(location {daml_helper_dist}) --strip-components=1 -C $$OUT/daml-helper

          mkdir -p $$OUT/daml2js
          tar xf $(location {daml2js_dist}) --strip-components=1 -C $$OUT/daml2js

          mkdir -p $$OUT/studio
          cp $(location {daml_extension}) $$OUT/studio/daml-bundled.vsix
          cp $(location {daml_extension_stylesheet}) $$OUT/studio/webview-stylesheet.css

          mkdir -p $$OUT/canton
          cp $(location {canton}) $$OUT/canton/canton.jar

          mkdir -p $$OUT/templates
          tar xf $(location {templates}) --strip-components=1 -C $$OUT/templates

          mkdir -p $$OUT/daml-sdk
          cp $(location {sdk_deploy_jar}) $$OUT/daml-sdk/daml-sdk.jar
          cp -L $(location {http_json_logback}) $$OUT/daml-sdk/json-api-logback.xml
          cp -L $(location {trigger_service_logback}) $$OUT/daml-sdk/
          cp -L $(location {oauth2_middleware_logback}) $$OUT/daml-sdk/
          cp -L $(location {navigator_logback}) $$OUT/daml-sdk/navigator-logback.xml
          cp -L $(location {java_codegen_logback}) $$OUT/daml-sdk/codegen-logback.xml
          cp -L $(location {trigger_logback}) $$OUT/daml-sdk/trigger-logback.xml
          cp -L $(location {daml_script_logback}) $$OUT/daml-sdk/script-logback.xml
          cp -L $(location {export_logback}) $$OUT/daml-sdk/export-logback.xml

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

def _protos_zip_impl(ctx):
    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    tmp_dir = ctx.actions.declare_directory("tmp_dir")
    zipper_args_file = ctx.actions.declare_file(
        ctx.label.name + ".zipper_args",
    )
    tools = [ctx.executable.tar, ctx.executable.gzip]
    ctx.actions.run_shell(
        inputs = [ctx.file.ledger_api_tarball] + ctx.files.daml_lf_tarballs,
        outputs = [tmp_dir],
        tools = tools,
        command = """
          set -eou pipefail
          export PATH=$PATH:{path}
          tar xf {ledger_api_tarball} -C {tmp_dir}
          for file in {lf_tarballs}
          do
              tar xf $file -C {tmp_dir}
          done
        """.format(
            ledger_api_tarball = ctx.file.ledger_api_tarball.path,
            tmp_dir = tmp_dir.path,
            lf_tarballs = " ".join([f.path for f in ctx.files.daml_lf_tarballs]),
            path = ":".join(["$PWD/`dirname {tool}`".format(tool = tool.path) for tool in tools]),
        ),
    )

    # zipper does not have an option to recursively zip files so we
    # use find to list the files.
    ctx.actions.run_shell(
        outputs = [zipper_args_file],
        inputs = [tmp_dir],
        command = """
        {find} -L {tmp_dir} -type f -printf "protos-{version}/%P=%p\n" > {args_file}
        """.format(
            version = sdk_version,
            find = posix.commands["find"],
            sed = posix.commands["sed"],
            tmp_dir = tmp_dir.path,
            args_file = zipper_args_file.path,
        ),
    )
    ctx.actions.run(
        outputs = [ctx.outputs.out],
        inputs = [zipper_args_file, tmp_dir],
        executable = ctx.executable.zipper,
        arguments = ["cC", ctx.outputs.out.path, "@" + zipper_args_file.path],
    )

protos_zip = rule(
    implementation = _protos_zip_impl,
    attrs = {
        "daml_lf_tarballs": attr.label_list(
            allow_files = True,
            default = [
                Label("//daml-lf/archive:daml_lf_{}_archive_proto_tar.tar.gz".format(version))
                for version in SUPPORTED_PROTO_STABLE_LF_VERSIONS + ["dev"]
            ],
        ),
        "ledger_api_tarball": attr.label(
            allow_single_file = True,
            default = Label("//canton:ledger_api_proto_tar.tar.gz"),
        ),
        "zipper": attr.label(
            default = Label("@bazel_tools//tools/zip:zipper"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
        "tar": attr.label(
            default = Label("@tar_dev_env//:tar"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
        "gzip": attr.label(
            default = Label("@gzip_dev_env//:gzip"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
    },
    outputs = {
        "out": "%{name}.zip",
    },
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
)
