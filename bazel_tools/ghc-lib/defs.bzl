# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_darwin")
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_binary", "haskell_cabal_library")
load(":version.bzl", "GHC_FLAVOR", "GHC_LIB_VERSION")

def ghc_lib_gen():
    native.filegroup(
        name = "srcs",
        srcs = native.glob(["**"]),
        visibility = ["//visibility:public"],
    )
    haskell_cabal_library(
        name = "ghc-lib-gen-lib",
        package_name = "ghc-lib-gen",
        version = "0.1.0.0",
        haddock = False,
        srcs = [":srcs"],
        deps = [
            "@stackage//:base",
            "@stackage//:process",
            "@stackage//:filepath",
            "@stackage//:containers",
            "@stackage//:directory",
            "@stackage//:optparse-applicative",
            "@stackage//:bytestring",
            "@stackage//:yaml",
            "@stackage//:aeson",
            "@stackage//:text",
            "@stackage//:unordered-containers",
            "@stackage//:extra",
        ],
    )
    haskell_cabal_binary(
        name = "ghc-lib-gen",
        srcs = [":srcs"],
        deps = [
            ":ghc-lib-gen-lib",
            "@stackage//:base",
            "@stackage//:containers",
            "@stackage//:directory",
            "@stackage//:extra",
            "@stackage//:filepath",
            "@stackage//:optparse-applicative",
            "@stackage//:process",
        ],
        visibility = ["//visibility:public"],
    )

def ghc():
    native.filegroup(
        name = "hadrian-srcs",
        srcs = native.glob(["hadrian/**"]),
        visibility = ["//visibility:public"],
    )
    haskell_cabal_binary(
        name = "hadrian",
        srcs = [":hadrian-srcs"],
        deps = [
            "@stackage//:base",
            "@stackage//:Cabal",
            "@stackage//:containers",
            "@stackage//:directory",
            "@stackage//:extra",
            "@stackage//:mtl",
            "@stackage//:parsec",
            "@stackage//:QuickCheck",
            "@stackage//:shake",
            "@stackage//:transformers",
            "@stackage//:unordered-containers",
        ],
        tools = [
            "@stackage-exe//alex",
            "@stackage-exe//happy",
        ],
        cabalopts = [
            "--ghc-option=-Wno-dodgy-imports",
            "--ghc-option=-Wno-unused-imports",
        ],
    )
    native.filegroup(
        name = "srcs",
        srcs = native.glob(["**"]),
        visibility = ["//visibility:public"],
    )
    for component in ["", "-parser"]:
        native.genrule(
            name = "ghc-lib{}".format(component),
            srcs = [
                ":srcs",
                ":README.md",
            ],
            tools = [
                "@ghc-lib-gen",
                "@//bazel_tools/ghc-lib:sh-lib",
                ":hadrian",
            ],
            toolchains = [
                "@rules_cc//cc:current_cc_toolchain",
                "@//bazel_tools/ghc-lib:libs",
                "@//bazel_tools/ghc-lib:tools",
            ],
            outs = [
                "ghc-lib{}.cabal".format(component),
                "ghc-lib{}-{}.tar.gz".format(component, GHC_LIB_VERSION),
            ],
            cmd = """\
set -euo pipefail
EXECROOT=$$PWD
. $(execpath @//bazel_tools/ghc-lib:sh-lib)

SEP="$$(path_list_separtor)"
export LIBRARY_PATH="$$(make_all_absolute "$(LIBS_LIBRARY_PATH)")"
export PATH="$$(make_all_absolute "$(TOOLS_PATH)")$$SEP$$PATH"
export PATH="$$(abs_dirname "$(execpath :hadrian)")$$SEP$$PATH"
export LANG={lang}
export CC="$$(make_absolute $(CC))"
export LD="$$(make_absolute $(LD))"

GHC="$$(abs_dirname $(execpath :README.md))"
TMP=$$(mktemp -d)
trap "rm -rf $$TMP" EXIT
cp -rLt $$TMP $$GHC/.

export HOME="$$TMP"
export STACK_ROOT="$$TMP/.stack"
mkdir -p $$STACK_ROOT
echo -e "system-ghc: true\\ninstall-ghc: false" > $$STACK_ROOT/config.yaml

$(execpath @ghc-lib-gen) $$TMP --ghc-lib{component} --ghc-flavor={ghc_flavor}
sed -i.bak \\
  -e 's#version: 0.1.0#version: {ghc_lib_version}#' \\
  $$TMP/ghc-lib{component}.cabal
cp $$TMP/ghc-lib{component}.cabal $(execpath ghc-lib{component}.cabal)
(cd $$TMP; cabal sdist -o $$EXECROOT/$(RULEDIR))
""".format(
                component = component,
                ghc_flavor = GHC_FLAVOR,
                ghc_lib_version = GHC_LIB_VERSION,
                lang = "en_US.UTF-8" if is_darwin else "C.UTF-8",
            ),
            visibility = ["//visibility:public"],
        )
