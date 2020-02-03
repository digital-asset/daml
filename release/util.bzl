# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def sdk_tarball(name, version):
    native.genrule(
        name = name,
        srcs = [
            ":sdk-config.yaml.tmpl",
            ":install.sh",
            ":install.bat",
            "//ledger/sandbox:src/main/resources/logback.xml",
            "//navigator/backend:src/main/resources/logback.xml",
            "//extractor:src/main/resources/logback.xml",
            "//ledger-service/http-json:release/json-api-logback.xml",
            "//language-support/java/codegen:src/main/resources/logback.xml",
            "//triggers/runner:src/main/resources/logback.xml",
            "//daml-script/runner:src/main/resources/logback.xml",
            "//:NOTICES",
            version,
            "//daml-assistant:daml-dist",
            "//compiler/damlc:damlc-dist",
            "//compiler/daml-extension:vsix",
            "//daml-assistant/daml-helper:daml-helper-dist",
            "//language-support/ts/codegen:daml2ts-dist",
            "//templates:templates-tarball.tar.gz",
            "//triggers/daml:daml-trigger.dar",
            "//daml-script/daml:daml-script.dar",
            "//daml-assistant/daml-sdk:sdk_deploy.jar",
        ],
        tools = ["@zip_dev_env//:zip"],
        outs = ["{}.tar.gz".format(name)],
        cmd = """
          # damlc
          VERSION=$$(cat $(location {version}))
          ZIP=$$PWD/$(location @zip_dev_env//:zip)
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

          # Patch the SDK version in all bundled DAML libraries.
          # This is necessary to make daml-sdk-head work despite the check in damlc
          # that DAR dependencies are built with the same SDK.
          for file in $$OUT/daml-libs/*; do
            file=$$PWD/$$file
            chmod +w $$file
            unzip -q $$file META-INF/MANIFEST.MF -d $$OUT/daml-libs
            sed -i "s/Sdk-Version:.*/Sdk-Version: $$VERSION/" $$OUT/daml-libs/META-INF/MANIFEST.MF
            (cd $$OUT/daml-libs; $$ZIP -q $$file META-INF/MANIFEST.MF)
            rm $$OUT/daml-libs/META-INF/MANIFEST.MF
          done

          mkdir -p $$OUT/daml-helper
          tar xf $(location //daml-assistant/daml-helper:daml-helper-dist) --strip-components=1 -C $$OUT/daml-helper

          mkdir -p $$OUT/daml2ts
          tar xf $(location //language-support/ts/codegen:daml2ts-dist) --strip-components=1 -C $$OUT/daml2ts

          mkdir -p $$OUT/studio
          cp $(location //compiler/daml-extension:vsix) $$OUT/studio/daml-bundled.vsix

          mkdir -p $$OUT/templates
          tar xf $(location //templates:templates-tarball.tar.gz) --strip-components=1 -C $$OUT/templates

          mkdir -p $$OUT/daml-sdk
          cp $(location //daml-assistant/daml-sdk:sdk_deploy.jar) $$OUT/daml-sdk/daml-sdk.jar
          cp -L $(location //ledger-service/http-json:release/json-api-logback.xml) $$OUT/daml-sdk/
          cp -L $(location //ledger/sandbox:src/main/resources/logback.xml) $$OUT/daml-sdk/sandbox-logback.xml
          cp -L $(location //navigator/backend:src/main/resources/logback.xml) $$OUT/daml-sdk/navigator-logback.xml
          cp -L $(location //extractor:src/main/resources/logback.xml) $$OUT/daml-sdk/extractor-logback.xml
          cp -L $(location //language-support/java/codegen:src/main/resources/logback.xml) $$OUT/daml-sdk/codegen-logback.xml
          cp -L $(location //triggers/runner:src/main/resources/logback.xml) $$OUT/daml-sdk/trigger-logback.xml
          cp -L $(location //daml-script/runner:src/main/resources/logback.xml) $$OUT/daml-sdk/script-logback.xml

          tar zcf $@ --format=ustar $$OUT
        """.format(version = version),
        visibility = ["//visibility:public"],
    )
