# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def sdk_tarball(name, version):
    native.genrule(
        name = name,
        srcs = [
            ":sdk-config.yaml.tmpl",
            ":install.sh",
            ":install.bat",
            "//ledger-service/http-json:release/json-api-logback.xml",
            "//:NOTICES",
            version,
            "//daml-assistant:daml-dist",
            "//compiler/damlc:damlc-dist",
            "//compiler/daml-extension:vsix",
            "//daml-assistant/daml-helper:daml-helper-dist",
            "//templates:templates-tarball.tar.gz",
            "//triggers/daml:daml-trigger.dar",
            "//daml-script/daml:daml-script.dar",
            "//daml-assistant/daml-sdk:sdk_deploy.jar",
        ],
        outs = ["{}.tar.gz".format(name)],
        cmd = """
          # damlc
          VERSION=$$(cat $(location {version}))
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
          cp -t $$OUT/daml-libs $(location //triggers/daml:daml-trigger.dar)
          cp -t $$OUT/daml-libs $(location //daml-script/daml:daml-script.dar)

          mkdir -p $$OUT/daml-helper
          tar xf $(location //daml-assistant/daml-helper:daml-helper-dist) --strip-components=1 -C $$OUT/daml-helper

          mkdir -p $$OUT/studio
          cp $(location //compiler/daml-extension:vsix) $$OUT/studio/daml-bundled.vsix

          mkdir -p $$OUT/templates
          tar xf $(location //templates:templates-tarball.tar.gz) --strip-components=1 -C $$OUT/templates

          mkdir -p $$OUT/daml-sdk
          cp $(location //daml-assistant/daml-sdk:sdk_deploy.jar) $$OUT/daml-sdk/daml-sdk.jar
          cp -L $(location //ledger-service/http-json:release/json-api-logback.xml) $$OUT/daml-sdk/

          tar zcf $@ --format=ustar $$OUT
        """.format(version = version),
        visibility = ["//visibility:public"],
    )
