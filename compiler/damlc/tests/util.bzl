# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:haskell.bzl", "da_haskell_test")
load("//bazel_tools/sh:sh.bzl", "sh_inline_test")
load("@build_environment//:configuration.bzl", "ghc_version", "sdk_version")

script_installer = """\
SCRIPTDAR=$$(canonicalize_rlocation $(rootpath //daml-script/daml:daml-script.dar))
cat <<EOF > "$$TMP/daml.yaml"
sdk-version: {sdk_version}
name: proj
version: 0.0.1
source: .
dependencies: [daml-prim, daml-stdlib, "$$SCRIPTDAR"]
EOF

importargs="--package-db=./.daml/package-database --package=daml-script-{ghc_version}"
(cd "$$TMP" && $$DAMLC build)
"""

def damlc_compile_test(
        name,
        srcs,
        main,
        damlc = "//compiler/damlc",
        stack_limit = "",
        heap_limit = "",
        enable_scripts = False,
        **kwargs):
    stack_opt = "-K" + stack_limit if stack_limit else ""
    heap_opt = "-M" + heap_limit if heap_limit else ""
    sh_inline_test(
        name = name,
        data = [damlc, main, "//daml-script/daml:daml-script.dar"] + srcs,
        cmd = """\
DAMLC=$$(canonicalize_rlocation $(rootpath {damlc}))
MAIN=$$(canonicalize_rlocation $(rootpath {main}))

TMP=$$(mktemp -d)
function cleanup() {{
  rm -rf "$$TMP"
}}
trap cleanup EXIT

importargs=""
{install_script}

DAML_PROJECT="$$TMP" $$DAMLC compile $$MAIN -o $$TMP/out $$importargs +RTS -s {stack_opt} {heap_opt}
""".format(
            damlc = damlc,
            main = main,
            stack_opt = stack_opt,
            heap_opt = heap_opt,
            install_script =
                script_installer.format(
                    sdk_version = sdk_version,
                    ghc_version = ghc_version,
                ) if enable_scripts else "",
        ),
        **kwargs
    )
