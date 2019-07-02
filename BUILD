package(default_visibility = ["//:__subpackages__"])

load(
    "@io_tweag_rules_haskell//haskell:haskell.bzl",
    "haskell_toolchain",
)
load(
    "@io_tweag_rules_haskell//haskell:c2hs.bzl",
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
    "@io_tweag_rules_haskell//haskell:c2hs.bzl",
    "c2hs_toolchain",
)

c2hs_toolchain(
    name = "c2hs-toolchain",
    c2hs = "@haskell_c2hs//:bin",
)

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
sdkVersion, componentVersion :: String
sdkVersion = "$$SDK_VERSION"
componentVersion = "$$COMPONENT_VERSION"
EOF
    """,
)

da_haskell_library(
    name = "sdk-version-hs-lib",
    srcs = [":sdk-version-hs"],
    hazel_deps = ["base"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "git-revision",
    outs = [".git-revision"],
    cmd = """
        grep '^STABLE_GIT_REVISION ' bazel-out/stable-status.txt | cut -d ' ' -f 2 > $@
    """,
    stamp = True,
)

#
# Common aliases
#

alias(
    name = "damlc",
    actual = "//daml-foundations/daml-tools/damlc-app:damlc-app",
)

alias(
    name = "damlc@ghci",
    actual = "//daml-foundations/daml-tools/damlc-app:damlc-app@ghci",
)

alias(
    name = "damlc-dist",
    actual = "//daml-foundations/daml-tools/damlc-app:damlc-dist",
)

alias(
    name = "hie-core",
    actual = "//compiler/hie-core:hie-core-exe",
) if not is_windows else None  # Disable on Windows until ghc-paths is fixed upstream

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
    "./3rdparty/jvm/*",
    "./3rdparty/workspace.bzl",
    "./hazel/packages.bzl",
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
    visibility = ["//visibility:public"],
    deps = [
        ":damlc",
        "//daml-assistant:daml",
        "//daml-assistant/daml-helper",
    ],
)
