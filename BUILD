package(default_visibility = ["//:__subpackages__"])

load("@bazel_tools//tools/python:toolchain.bzl", "py_runtime_pair")
load(
    "@rules_haskell//haskell:defs.bzl",
    "haskell_toolchain",
)
load(
    "@rules_haskell//haskell:c2hs.bzl",
    "c2hs_toolchain",
)
load("//bazel_tools:haskell.bzl", "da_haskell_library", "da_haskell_repl")
load("@os_info//:os_info.bzl", "is_windows")

exports_files([".hlint.yaml"])

config_setting(
    name = "on_linux",
    constraint_values = [
        "@bazel_tools//platforms:linux",
    ],
)

config_setting(
    name = "on_osx",
    constraint_values = [
        "@bazel_tools//platforms:osx",
    ],
)

config_setting(
    name = "on_freebsd",
    constraint_values = [
        "@bazel_tools//platforms:freebsd",
    ],
)

config_setting(
    name = "on_windows",
    constraint_values = [
        "@bazel_tools//platforms:windows",
    ],
)

config_setting(
    name = "profiling_build",
    values = {
        "compilation_mode": "dbg",
    },
)

load(
    "@rules_haskell//haskell:c2hs.bzl",
    "c2hs_toolchain",
)

c2hs_toolchain(
    name = "c2hs-toolchain",
    c2hs = "@haskell_c2hs//:bin",
)

#
# Python toolchain
#

py_runtime(
    name = "nix_python3_runtime",
    interpreter = "@python3_nix//:bin/python",
    python_version = "PY3",
) if not is_windows else None

py_runtime_pair(
    name = "nix_python_runtime_pair",
    py3_runtime = ":nix_python3_runtime",
) if not is_windows else None

toolchain(
    name = "nix_python_toolchain",
    exec_compatible_with = [
        "@rules_haskell//haskell/platforms:nixpkgs",
    ],
    toolchain = ":nix_python_runtime_pair",
    toolchain_type = "@bazel_tools//tools/python:toolchain_type",
) if not is_windows else None

filegroup(
    name = "node_modules",
    srcs = glob(["node_modules/**/*"]),
)

config_setting(
    name = "ghci_data",
    define_values = {
        "ghci_data": "True",
    },
)

config_setting(
    name = "hie_bios_ghci",
    define_values = {
        "hie_bios_ghci": "True",
    },
)

#
# Metadata
#

# The VERSION file is inlined in a few builds.
exports_files([
    "NOTICES",
    "LICENSE",
    "VERSION",
    "CHANGELOG",
    "tsconfig.json",
])

# FIXME(#448): We're currently assigning version (100+x).y.z to all components
# in SDK version x.y.z. As long as x < 10, 10x.y.z == (100+x).y.z.  Since we'll
# stop splitting the SDK into individual components _very_ soon, this rule
# will not survive until x >= 10.
genrule(
    name = "component-version",
    srcs = ["VERSION"],
    outs = ["COMPONENT-VERSION"],
    cmd = """
        echo -n 10 > $@
        cat $(location VERSION) >> $@
    """,
)

genrule(
    name = "sdk-version-hs",
    srcs = [
        "VERSION",
        ":component-version",
    ],
    outs = ["SdkVersion.hs"],
    cmd = """
        SDK_VERSION=$$(cat $(location VERSION))
        COMPONENT_VERSION=$$(cat $(location :component-version))
        cat > $@ <<EOF
module SdkVersion where
sdkVersion, componentVersion, damlStdlib :: String
sdkVersion = "$$SDK_VERSION"
componentVersion = "$$COMPONENT_VERSION"
damlStdlib = "daml-stdlib-" ++ sdkVersion
EOF
    """,
)

da_haskell_library(
    name = "sdk-version-hs-lib",
    srcs = [":sdk-version-hs"],
    hackage_deps = ["base"],
    visibility = ["//visibility:public"],
)

#
# Common aliases
#

alias(
    name = "damlc",
    actual = "//compiler/damlc:damlc",
)

alias(
    name = "damlc@ghci",
    actual = "//compiler/damlc:damlc@ghci",
)

alias(
    name = "damlc-dist",
    actual = "//compiler/damlc:damlc-dist",
)

alias(
    name = "daml2ts",
    actual = "//language-support/ts/codegen:daml2ts",
)

alias(
    name = "daml2ts@ghci",
    actual = "//language-support/ts/codegen:daml2ts@ghci",
)

alias(
    name = "daml-lf-repl",
    actual = "//daml-lf/repl:repl",
)

alias(
    name = "bindings-java",
    actual = "//language-support/java/bindings:bindings-java",
)

exports_files([
    ".scalafmt.conf",
])

# Buildifier.

load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")

buildifier_excluded_patterns = [
    "./3rdparty/haskell/c2hs-package.bzl",
    "./3rdparty/haskell/network-package.bzl",
    "./hazel/packages.bzl",
    "./node_modules/*",
]

# Run this to check if BUILD files are well-formatted.
buildifier(
    name = "buildifier",
    exclude_patterns = buildifier_excluded_patterns,
    mode = "check",
)

# Run this to fix the errors in BUILD files.
buildifier(
    name = "buildifier-fix",
    exclude_patterns = buildifier_excluded_patterns,
    mode = "fix",
    verbose = True,
)

# Default target for da-ghci, da-ghcid.
da_haskell_repl(
    name = "repl",
    testonly = True,
    visibility = ["//visibility:public"],
    deps = [
        ":damlc",
        "//compiler/damlc/tests:generate-simple-dalf",
        "//daml-assistant:daml",
        "//daml-assistant/daml-helper",
        "//daml-assistant/integration-tests",
    ],
)
