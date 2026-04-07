load(
    "@//bazel_tools/ghc-lib:version.bzl",
    "GHC_CPP_OPTIONS",
    "GHC_FLAVOR",
    "GHC_LIB_VERSION",
)
load("@os_info//:os_info.bzl", "is_darwin")
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_binary")

filegroup(
    name = "hadrian-srcs",
    srcs = glob(["hadrian/**"]),
    visibility = ["//visibility:public"],
)

haskell_cabal_binary(
    name = "hadrian",
    flags = ["with_bazel"],
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

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

_LANG = "en_US.UTF-8" if is_darwin else "C.UTF-8"
_CPP_OPTIONS = " ".join(["--cpp={}".format(cpp) for cpp in GHC_CPP_OPTIONS])

genrule(
    name = "ghc-lib",
    srcs = [
        ":srcs",
        ":README.md",
        "@autoconf//:srcs",
        "@autoconf//:README",
        "@automake//:srcs",
        "@automake//:README",
    ],
    tools = [
        ":hadrian",
        "@ghc-lib-gen//:ghc-lib-gen",
        "@m4//:m4",
        "@rules_perl//:current_toolchain",
        "@rules_haskell//haskell:current_haskell_toolchain",
    ],
    toolchains = [
        "@rules_cc//cc:current_cc_toolchain",
    ],
    outs = [
        "ghc-lib.cabal",
        "ghc-lib-{version}.tar.gz".format(version = GHC_LIB_VERSION),
    ],
    cmd = """\
set -euo pipefail
ORIGIN=$$PWD

echo "HASKELL: $(locations @rules_haskell//haskell:current_haskell_toolchain)" | tr ' ' '\n' | grep bin/ghc | head -3
exit

# Tool paths
M4_DIR="$$(dirname $$(realpath $(execpath @m4//:m4)))"
PERL_DIR="$$(realpath $$(dirname $$(echo "$(locations @rules_perl//:current_toolchain)" | cut -d' ' -f1)))/bin"
AUTOCONF_SRC="$$ORIGIN/$$(dirname $(location @autoconf//:README))"

export PATH="$$PERL_DIR:$$M4_DIR:$$PATH"

# Build autoconf inline
AUTOCONF_PREFIX=$$(mktemp -d)
cd $$AUTOCONF_SRC
./configure --prefix=$$AUTOCONF_PREFIX
make install
cd $$ORIGIN
export PATH="$$AUTOCONF_PREFIX/bin:$$PATH"

# Build automake inline (shares prefix with autoconf)
AUTOMAKE_SRC="$$ORIGIN/$$(dirname $(location @automake//:README))"
cd $$AUTOMAKE_SRC
./configure --prefix=$$AUTOCONF_PREFIX
make install
cd $$ORIGIN

export LANG={lang}
export CC="$(CC)"
export LD="$(LD)"
export PATH="$$ORIGIN/$$(dirname $(execpath :hadrian)):$$PATH"

GHC_DIR="$$ORIGIN/$$(dirname $(execpath :README.md))"
TMP=$$(mktemp -d)
trap "rm -rf $$TMP $$AUTOCONF_PREFIX" EXIT
cp -rLt $$TMP $$GHC_DIR/.
export HOME="$$TMP"

$(execpath @ghc-lib-gen//:ghc-lib-gen) $$TMP \
    --ghc-lib \
    --ghc-flavor={ghc_flavor} \
    {cpp_options}

sed -i.bak -e "s#$$ORIGIN/##" $$TMP/ghc-lib/stage0/lib/settings
sed -i.bak -e 's#version: 0.1.0#version: {ghc_lib_version}#' $$TMP/ghc-lib.cabal

cp $$TMP/ghc-lib.cabal $(execpath ghc-lib.cabal)
# cabal sdist must run from the source tree; OLDPWD retains the exec root after cd.
cd $$TMP && cabal sdist -o $$OLDPWD/$(RULEDIR)
""".format(
        cpp_options = _CPP_OPTIONS,
        ghc_flavor = GHC_FLAVOR,
        ghc_lib_version = GHC_LIB_VERSION,
        lang = _LANG,
    ),
    visibility = ["//visibility:public"],
)
