# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools/daml_script:daml_script.bzl", "daml_script_test")
load("//bazel_tools:versions.bzl", "versions")

def _build_dar(
        name,
        package_name,
        srcs,
        data_dependencies,
        sdk_version,
        stable_script_support = False,
        run_unknown_failure_test = False):
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
DAML_CACHE=$$(mktemp -d)
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
  - daml-stdlib
  - daml-script
data-dependencies:$$DATA_DEPS
build-options:
  {stable_script_support_opt}
EOF
# TODO(dpm#12) Dpm doesn't support building a package via any kind of `--package-root` flag, so we must CD for now. Revert back to a flag once dpm supports this
PREV_PWD=$$PWD
cd $$TMP_DIR
DAML_CACHE=$$DAML_CACHE $$PREV_PWD/$(location {daml}) build -o $$PREV_PWD/$(OUTS)
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
            stable_script_support_opt = "- --ghc-option=-DSTABLE_SCRIPT_SUPPORT" if stable_script_support else "",
            run_unknown_failure_test_opt = "- --ghc-option=-DRUN_UNKNOWN_FAILURE_TEST" if run_unknown_failure_test else "",
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

# This test ensures cross-sdk compatibility with daml-script versions that support it
# and otherwise checks only this regression: https://github.com/digital-asset/daml/issues/14291
def data_dependencies_daml_script_test(old_sdk_version, run_unknown_failure_test = False):
    name = "data-dependencies-script-0.0.0-on-{old_sdk_version}{ext}".format(
        old_sdk_version = old_sdk_version,
        ext = "" if not run_unknown_failure_test else "-unknown-failure",
    )

    # For some reason, is_at_least(a, b) runs checks b >= a, so counter-intuitively, this
    # checks the old_sdk_version is at least 3.6.0
    supports_stable_script = versions.is_at_least("3.6.0", old_sdk_version)

    if run_unknown_failure_test and not supports_stable_script:
        fail("Cannot run unknown failure test on an SDK version that does not support stable daml-script.")

    _build_dar(
        name = name,
        package_name = "data-dependencies-script",
        srcs = ["//bazel_tools/data_dependencies:daml_script_test/ScriptExampleWrapper.daml"],
        data_dependencies = [
            "//:script-example-dar-{old_sdk_version}{ext}".format(
                old_sdk_version = old_sdk_version,
                ext = "" if not run_unknown_failure_test else "-with-script-override",
            ),
        ],
        stable_script_support = supports_stable_script,
        run_unknown_failure_test = run_unknown_failure_test,
        sdk_version = "0.0.0",
    )

    _validate_dar(
        name = name + "-validate",
        dar_name = name,
        sdk_version = "0.0.0",
    )

    daml_script_test(
        name = "data-dependencies-daml-script-from-{old_sdk_version}-test".format(
            old_sdk_version = old_sdk_version,
        ),
        runner_version = "0.0.0",
        compiler_version = "0.0.0",
        compiled_dar = name,
        script_name = "ScriptExampleWrapper:mainWrapper",
    )
