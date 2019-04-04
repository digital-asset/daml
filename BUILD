package(default_visibility = ["//:__subpackages__"])

load("@io_tweag_rules_haskell//haskell:haskell.bzl",
  "haskell_toolchain",
)
load("@io_tweag_rules_haskell//haskell:c2hs.bzl",
  "c2hs_toolchain",
)

load ("//bazel_tools:haskell.bzl", "da_haskell_library")

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

load(
    "@io_tweag_rules_haskell//haskell:c2hs.bzl",
    "c2hs_toolchain",
)


c2hs_toolchain(
    name = "c2hs-toolchain",
    c2hs = "@c2hs//:bin",
)

filegroup(name = "node_modules", srcs = glob(["node_modules/**/*"]))

config_setting(
    name = "ghci_data",
    define_values = {
        "ghci_data": "True",
    },
)

#
# Metadata
#

# The VERSION file is inlined in a few builds.
exports_files(["NOTICES", "LICENSE", "VERSION", "CHANGELOG", "tsconfig.json"])

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
    srcs = ["VERSION", ":component-version"],
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
    stamp = True,
    outs = [".git-revision"],
    cmd = """
        grep '^STABLE_GIT_REVISION ' bazel-out/stable-status.txt | cut -d ' ' -f 2 > $@
    """,
)


#
# Common aliases
#

alias(
  name = "damli",
  actual = "//daml-foundations/daml-tools/da-hs-damli-app:damli"
)

alias(
  name = "damli@ghci",
  actual = "//daml-foundations/daml-tools/da-hs-damli-app:damli@ghci"
)

alias(
  name = "damli-dist",
  actual = "//daml-foundations/daml-tools/da-hs-damli-app:dist"
)

alias(
  name = "damlc",
  actual = "//daml-foundations/daml-tools/da-hs-damlc-app:da-hs-damlc-app"
)

alias(
  name = "damlc@ghci",
  actual = "//daml-foundations/daml-tools/da-hs-damlc-app:da-hs-damlc-app@ghci"
)

alias(
  name = "damlc-dist",
  actual = "//daml-foundations/daml-tools/da-hs-damlc-app:damlc-dist"
)

alias(
  name = "daml-lf-repl",
  actual = "//daml-lf/repl:repl"
)

alias(
  name = "bindings-java",
  actual = "//language-support/java/bindings:bindings-java"
)

exports_files([
  ".scalafmt.conf"
])
