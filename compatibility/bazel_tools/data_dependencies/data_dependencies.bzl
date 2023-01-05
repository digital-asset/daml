# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools/daml_script:daml_script.bzl", "daml_script_test")

def _build_dar(
        name,
        package_name,
        srcs,
        data_dependencies,
        sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    native.genrule(
        name = name,
        srcs = srcs + data_dependencies,
        outs = ["%s.dar" % name],
        tools = [daml],
        cmd = """\
set -euo pipefail
TMP_DIR=$$(mktemp -d)
cleanup() {{ rm -rf $$TMP_DIR; }}
trap cleanup EXIT
mkdir -p $$TMP_DIR/src $$TMP_DIR/dep
for src in {srcs}; do
  cp -L $$src $$TMP_DIR/src
done
DATA_DEPS=
for dep in {data_dependencies}; do
  cp -L $$dep $$TMP_DIR/dep
  DATA_DEPS="$$DATA_DEPS\n  - dep/$$(basename $$dep)"
done
cat <<EOF >$$TMP_DIR/daml.yaml
sdk-version: {sdk_version}
name: {name}
source: src
version: 0.0.1
dependencies:
  - daml-prim
  - daml-script
data-dependencies:$$DATA_DEPS
EOF
$(location {daml}) build --project-root=$$TMP_DIR -o $$PWD/$(OUTS)
""".format(
            daml = daml,
            name = package_name,
            data_dependencies = " ".join([
                "$(location %s)" % dep
                for dep in data_dependencies
            ]),
            sdk_version = sdk_version,
            srcs = " ".join([
                "$(locations %s)" % src
                for src in srcs
            ]),
        ),
    )

def _validate_dar(
        name,
        dar_name,
        sdk_version):
    daml = "@daml-sdk-{sdk_version}//:daml".format(
        sdk_version = sdk_version,
    )
    native.sh_test(
        name = name,
        srcs = ["//bazel_tools/data_dependencies:validate_dar.sh"],
        args = [
            "$(rootpath %s)" % daml,
            "$(rootpath %s)" % dar_name,
        ],
        data = [daml, dar_name],
        deps = ["@bazel_tools//tools/bash/runfiles"],
    )

def data_dependencies_coins(sdk_version):
    """Build the coin1 and coin2 packages with the given SDK version.
    """
    _build_dar(
        name = "data-dependencies-coin1-{sdk_version}".format(
            sdk_version = sdk_version,
        ),
        package_name = "data-dependencies-coin1",
        srcs = ["//bazel_tools/data_dependencies:example/CoinV1.daml"],
        data_dependencies = [],
        sdk_version = sdk_version,
    )
    _build_dar(
        name = "data-dependencies-coin2-{sdk_version}".format(
            sdk_version = sdk_version,
        ),
        package_name = "data-dependencies-coin2",
        srcs = ["//bazel_tools/data_dependencies:example/CoinV2.daml"],
        data_dependencies = [],
        sdk_version = sdk_version,
    )

def data_dependencies_upgrade_test(old_sdk_version, new_sdk_version):
    """Build and validate the coin-upgrade package using the new SDK version.

    The package will have data-dependencies on the coin1 and coin2 package
    built with the old SDK version.
    """
    dar_name = "data-dependencies-upgrade-old-{old_sdk_version}-new-{new_sdk_version}".format(
        old_sdk_version = old_sdk_version,
        new_sdk_version = new_sdk_version,
    )
    _build_dar(
        name = dar_name,
        package_name = "data-dependencies-upgrade",
        srcs = ["//bazel_tools/data_dependencies:example/UpgradeFromCoinV1.daml"],
        data_dependencies = [
            "data-dependencies-coin1-{sdk_version}".format(
                sdk_version = old_sdk_version,
            ),
            "data-dependencies-coin2-{sdk_version}".format(
                sdk_version = old_sdk_version,
            ),
        ],
        sdk_version = new_sdk_version,
    )
    _validate_dar(
        name = "data-dependencies-test-old-{old_sdk_version}-new-{new_sdk_version}".format(
            old_sdk_version = old_sdk_version,
            new_sdk_version = new_sdk_version,
        ),
        dar_name = dar_name,
        sdk_version = new_sdk_version,
    )

# regression test for https://github.com/digital-asset/daml/issues/14291
def data_dependencies_daml_script_test(old_sdk_version):
    data_dep_name = "data-dependencies-script1-{old_sdk_version}".format(
        old_sdk_version = old_sdk_version,
    )
    main_name = "data-dependencies-script2-from-{old_sdk_version}".format(
        old_sdk_version = old_sdk_version,
    )

    _build_dar(
        name = data_dep_name,
        package_name = "data-dependencies-script1",
        srcs = ["//bazel_tools/data_dependencies:daml_script_test/Dep.daml"],
        sdk_version = old_sdk_version,
        data_dependencies = [],
    )

    _build_dar(
        name = main_name,
        package_name = "data-dependencies-script2",
        srcs = ["//bazel_tools/data_dependencies:daml_script_test/Main.daml"],
        data_dependencies = [
            data_dep_name,
        ],
        sdk_version = "0.0.0",
    )

    _validate_dar(
        name = main_name + "-validate",
        dar_name = main_name,
        sdk_version = "0.0.0",
    )

    daml_script_test(
        name = "data-dependencies-daml-script-from-{old_sdk_version}-test-1".format(
            old_sdk_version = old_sdk_version,
        ),
        runner_version = "0.0.0",
        compiler_version = "0.0.0",
        compiled_dar = main_name,
        script_name = "Main:run1",
    )

    daml_script_test(
        name = "data-dependencies-daml-script-from-{old_sdk_version}-test-2".format(
            old_sdk_version = old_sdk_version,
        ),
        runner_version = "0.0.0",
        compiler_version = "0.0.0",
        compiled_dar = main_name,
        script_name = "Main:run2",
    )
