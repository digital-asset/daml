# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//daml-lf/language:daml-lf.bzl", "LF_VERSIONS")
load("@build_environment//:configuration.bzl", "sdk_version")

def sdk_tarball(name, version):
    native.genrule(
        name = name,
        srcs = [
            ":sdk-config.yaml.tmpl",
            ":install.sh",
            ":install.bat",
            "//ledger/sandbox-common:src/main/resources/logback.xml",
            "//navigator/backend:src/main/resources/logback.xml",
            "//extractor:src/main/resources/logback.xml",
            "//ledger-service/http-json:src/main/resources/logback.xml",
            "//triggers/service/auth:release/oauth2-middleware-logback.xml",
            "//triggers/service:release/trigger-service-logback.xml",
            "//language-support/java/codegen:src/main/resources/logback.xml",
            "//triggers/runner:src/main/resources/logback.xml",
            "//daml-script/runner:src/main/resources/logback.xml",
            "//:NOTICES",
            "//daml-assistant:daml-dist",
            "//compiler/damlc:damlc-dist",
            "//compiler/daml-extension:vsix",
            "//daml-assistant/daml-helper:daml-helper-dist",
            "//language-support/ts/codegen:daml2js-dist",
            "//templates:templates-tarball.tar.gz",
            "//triggers/daml:daml-trigger-dars",
            "//daml-script/daml:daml-script-dars",
            "//daml-assistant/daml-sdk:sdk_deploy.jar",
        ],
        outs = ["{}.tar.gz".format(name)],
        tools = ["//bazel_tools/sh:mktgz"],
        cmd = """
          # damlc
          VERSION={version}
          OUT=sdk-$$VERSION
          mkdir -p $$OUT

          cp $(location //:NOTICES) $$OUT/NOTICES

          cp $(location :install.sh) $$OUT/install.sh
          cp $(location :install.bat) $$OUT/install.bat

          cp $(location :sdk-config.yaml.tmpl) $$OUT/sdk-config.yaml
          sed -i "s/__VERSION__/$$VERSION/" $$OUT/sdk-config.yaml

          mkdir -p $$OUT/daml
          tar xf $(location //daml-assistant:daml-dist) --strip-components=1 -C $$OUT/daml

          mkdir -p $$OUT/damlc
          tar xf $(location //compiler/damlc:damlc-dist) --strip-components=1 -C $$OUT/damlc

          mkdir -p $$OUT/daml-libs
          cp -t $$OUT/daml-libs $(locations //triggers/daml:daml-trigger-dars)
          cp -t $$OUT/daml-libs $(locations //daml-script/daml:daml-script-dars)

          mkdir -p $$OUT/daml-helper
          tar xf $(location //daml-assistant/daml-helper:daml-helper-dist) --strip-components=1 -C $$OUT/daml-helper

          mkdir -p $$OUT/daml2js
          tar xf $(location //language-support/ts/codegen:daml2js-dist) --strip-components=1 -C $$OUT/daml2js

          mkdir -p $$OUT/studio
          cp $(location //compiler/daml-extension:vsix) $$OUT/studio/daml-bundled.vsix

          mkdir -p $$OUT/templates
          tar xf $(location //templates:templates-tarball.tar.gz) --strip-components=1 -C $$OUT/templates

          mkdir -p $$OUT/daml-sdk
          cp $(location //daml-assistant/daml-sdk:sdk_deploy.jar) $$OUT/daml-sdk/daml-sdk.jar
          cp -L $(location //ledger-service/http-json:src/main/resources/logback.xml) $$OUT/daml-sdk/json-api-logback.xml
          cp -L $(location //triggers/service:release/trigger-service-logback.xml) $$OUT/daml-sdk/
          cp -L $(location //triggers/service/auth:release/oauth2-middleware-logback.xml) $$OUT/daml-sdk/
          cp -L $(location //ledger/sandbox-common:src/main/resources/logback.xml) $$OUT/daml-sdk/sandbox-logback.xml
          cp -L $(location //navigator/backend:src/main/resources/logback.xml) $$OUT/daml-sdk/navigator-logback.xml
          cp -L $(location //extractor:src/main/resources/logback.xml) $$OUT/daml-sdk/extractor-logback.xml
          cp -L $(location //language-support/java/codegen:src/main/resources/logback.xml) $$OUT/daml-sdk/codegen-logback.xml
          cp -L $(location //triggers/runner:src/main/resources/logback.xml) $$OUT/daml-sdk/trigger-logback.xml
          cp -L $(location //daml-script/runner:src/main/resources/logback.xml) $$OUT/daml-sdk/script-logback.xml

          $(execpath //bazel_tools/sh:mktgz) $@ $$OUT
        """.format(version = version),
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
                for version in LF_VERSIONS
            ],
        ),
        "ledger_api_tarball": attr.label(
            allow_single_file = True,
            default = Label("//ledger-api/grpc-definitions:ledger_api_proto_tar.tar.gz"),
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
