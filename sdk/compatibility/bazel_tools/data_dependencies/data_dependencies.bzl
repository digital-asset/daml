# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools/daml_script:daml_script.bzl", "daml_script_test")
load("@daml//bazel_tools/sh:sh.bzl", "sh_inline_test")
load(
    "@daml//bazel_tools/client_server:client_server_test.bzl",
    "client_server_test",
)

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

def data_dependencies_codegen_test(new_sdk_version, old_sdk_version):
    name = "data-dependencies-codegen-{new_sdk_version}-depends-on-{old_sdk_version}".format(
        old_sdk_version = old_sdk_version,
        new_sdk_version = new_sdk_version,
    )

    _build_dar(
      name = name + "-main",
      package_name = "codegen-main",
      data_dependencies = [
          ":" + name + "-dep",
      ],
      sdk_version = new_sdk_version,
      srcs = ["//bazel_tools/data_dependencies:codegen_test/main/Main.daml"],
    )

    _build_dar(
      name = name + "-dep",
      package_name = "codegen-dep",
      data_dependencies = [],
      sdk_version = old_sdk_version,
      srcs = ["//bazel_tools/data_dependencies:codegen_test/dep/Dep.daml"],
    )

    daml_old = "@daml-sdk-{old_sdk_version}//:daml".format(
        old_sdk_version = old_sdk_version,
    )

    daml_new = "@daml-sdk-{new_sdk_version}//:daml".format(
        new_sdk_version = new_sdk_version,
    )

    native.genrule(
        name = name + "-client-sh",
        outs = [name + "-client.sh"],
        tools = [
            daml_old,
            daml_new,
            ":" + name + "-main.dar",
            "//bazel_tools/data_dependencies:codegen_test/openapi.yaml",
            "//bazel_tools/data_dependencies:codegen_test/package.json",
            "//bazel_tools/data_dependencies:codegen_test/tsconfig.json",
            "//bazel_tools/data_dependencies:codegen_test/index.ts",
            "@yarn//:yarn",
            "@nodejs//:node",
            "@nodejs//:npm",
            "@head_sdk//:community_app_deploy.jar",
        ],
        cmd = """\
cat >$(OUTS) <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
canonicalize_rlocation() {{
  # Note (MK): This is a fun one: Let's say $$TEST_WORKSPACE is "compatibility"
  # and the argument points to a target from an external workspace, e.g.,
  # @daml-sdk-0.0.0//:daml. Then the short path will point to
  # ../daml-sdk-0.0.0/daml. Putting things together we end up with
  # compatibility/../daml-sdk-0.0.0/daml. On Linux and MacOS this works
  # just fine. However, on windows we need to normalize the path
  # or rlocation will fail to find the path in the manifest file.
  rlocation $$(realpath -L -s -m --relative-to=$$PWD $$TEST_WORKSPACE/$$1)
}}

get_exe() {{
  if [[ %os% = windows ]]; then
    for arg in "$$@"; do
      if [[ $$arg = *.exe ]]; then
        echo "$$arg"
        return
      fi
    done
    echo "$$1"
  else
    echo "$$1"
  fi
}}

yarn=$$(canonicalize_rlocation $$(get_exe $(rootpaths {yarn})))
node=$$(canonicalize_rlocation $$(get_exe $(rootpaths {node})))
npm=$$(canonicalize_rlocation $$(get_exe $(rootpaths {npm})))
canton_jar=$$(canonicalize_rlocation $$(get_exe $(rootpaths {community_app_deploy})))

daml_new=$$(canonicalize_rlocation $$(get_exe $(rootpaths {daml_new})))
daml_old=$$(canonicalize_rlocation $$(get_exe $(rootpaths {daml_old})))

openapi=$$(canonicalize_rlocation $$(get_exe $(rootpaths {openapi})))
packagejson=$$(canonicalize_rlocation $$(get_exe $(rootpaths {packagejson})))
tsconfig=$$(canonicalize_rlocation $$(get_exe $(rootpaths {tsconfig})))
indexts=$$(canonicalize_rlocation $$(get_exe $(rootpaths {indexts})))

timeout=60
while [ ! -e _port_file ]; do
    if [ "$$timeout" = 0 ]; then
        echo "Timed out waiting for Canton startup" >&2
        exit 1
    fi
    sleep 1
    timeout=$$((timeout - 1))
done

mkdir -p ./client/src
cp $$openapi ./client
cp $$packagejson ./client
cp $$tsconfig ./client

cp $$indexts ./client/src
cp $$(canonicalize_rlocation $(rootpaths {dar})) ./client/target.dar

$$daml_new codegen js -o ./client/codegen ./client/target.dar

cd client
rm -rf dist
$$yarn install
$$npm run build
$$node dist/index.js
EOF
chmod +x $(OUTS)
""".format(
            daml_old = daml_old,
            daml_new = daml_new,
            dar = ":" + name + "-main.dar",
            old_sdk_version = old_sdk_version,
            new_sdk_version = new_sdk_version,
            openapi = "//bazel_tools/data_dependencies:codegen_test/openapi.yaml",
            packagejson = "//bazel_tools/data_dependencies:codegen_test/package.json",
            tsconfig = "//bazel_tools/data_dependencies:codegen_test/tsconfig.json",
            indexts = "//bazel_tools/data_dependencies:codegen_test/index.ts",
            yarn = "@yarn//:yarn",
            node = "@nodejs//:node",
            npm = "@nodejs//:npm",
            npx = "@nodejs//:npx_bin",
            community_app_deploy = "@head_sdk//:community_app_deploy.jar",
        ),
    )

    native.sh_binary(
        name = name + "-client",
        srcs = [name + "-client.sh"],
        data = [
            daml_old,
            daml_new,
            ":" + name + "-main.dar",
            "//bazel_tools/data_dependencies:codegen_test/openapi.yaml",
            "//bazel_tools/data_dependencies:codegen_test/package.json",
            "//bazel_tools/data_dependencies:codegen_test/tsconfig.json",
            "//bazel_tools/data_dependencies:codegen_test/index.ts",
            "@yarn//:yarn",
            "@nodejs//:node",
            "@nodejs//:npm",
            "@head_sdk//:community_app_deploy.jar",
        ],
    )

    daml_runner = "@daml-sdk-{version}//:daml".format(
        version = "0.0.0",
    )
    server = daml_runner
    server_args = ["sandbox", "--canton-port-file", "_port_file", "--json-api-port", "7575"]
    server_files = []
    server_files_prefix = "--dar="

    client_server_test(
        name = name,
        client = "{}-client".format(name),
        client_args = [],
        client_files = [],
        data = [],
        runner = "//bazel_tools/client_server:runner",
        runner_args = ["6865"],
        server = server,
        server_args = server_args,
        server_files = server_files,
        server_files_prefix = server_files_prefix,
        tags = ["exclusive"],
    )

# regression test for https://github.com/digital-asset/daml/issues/14291
def data_dependencies_daml_script_test(old_sdk_version, new_sdk_version):
    name = "data-dependencies-daml-script-{new_sdk_version}-depends-on-{old_sdk_version}".format(
        old_sdk_version = old_sdk_version,
        new_sdk_version = new_sdk_version,
    )
    main_name = name + "-main"
    dep_name = name + "-dep"

    _build_dar(
        name = dep_name,
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
            dep_name,
        ],
        sdk_version = new_sdk_version,
    )

    _validate_dar(
        name = main_name + "-validate",
        dar_name = main_name,
        sdk_version = new_sdk_version,
    )

    daml_script_test(
        name = name + "-test-1",
        runner_version = new_sdk_version,
        compiler_version = new_sdk_version,
        compiled_dar = main_name,
        script_name = "Main:run1",
    )

    daml_script_test(
        name = name + "-test-2",
        runner_version = new_sdk_version,
        compiler_version = new_sdk_version,
        compiled_dar = main_name,
        script_name = "Main:run2",
    )
