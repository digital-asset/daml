package(default_visibility = ["//:__subpackages__"])

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
load("@build_environment//:configuration.bzl", "ghc_version", "mvn_version", "sdk_version")

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
    c2hs = "@c2hs//:c2hs",
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
    "CHANGELOG",
    "tsconfig.json",
])

genrule(
    name = "mvn_version_file",
    outs = ["MVN_VERSION"],
    cmd = "echo -n {mvn} > $@".format(mvn = mvn_version),
)

genrule(
    name = "sdk-version-hs",
    srcs = [],
    outs = ["SdkVersion.hs"],
    cmd = """
        cat > $@ <<EOF
module SdkVersion where

import Module (stringToUnitId, UnitId)
import qualified Data.List.Split as Split
import qualified Data.List.Extra as List.Extra

sdkVersion :: String
sdkVersion = "{sdk}"

mvnVersion :: String
mvnVersion = "{mvn}"

damlStdlib :: UnitId
damlStdlib = stringToUnitId ("daml-stdlib-" ++ "{ghc}")

-- | Turns a SemVer string into one suitable for ghc-pkg
--
-- The DAML SDK uses semantic versioning, while internally we need a version
-- string that ghc-pkg can understand. We do that by removing the '-snapshot'
-- qualifier when present.
--
-- Expected version strings:
-- 0.13.51 -> release, no change
-- 0.13.51-snapshot.20200212.3024.04e6fa2c -> snapshot release, change to
--                                         0.13.51.20200212.3024 internally
-- This logic must stay in sync with bazel_tools/build_environment.bzl.
toGhcPkgVersion :: String -> String
toGhcPkgVersion rawVersion =
    case Split.splitOn "-snapshot" rawVersion of
      [v] -> v
      [v, pr] -> v ++ List.Extra.dropEnd 9 pr
      _ -> rawVersion
EOF
    """.format(
        ghc = ghc_version,
        mvn = mvn_version,
        sdk = sdk_version,
    ),
)

da_haskell_library(
    name = "sdk-version-hs-lib",
    srcs = [":sdk-version-hs"],
    hackage_deps = [
        "base",
        "extra",
        "ghc-lib-parser",
        "split",
    ],
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
    name = "daml2js",
    actual = "//language-support/ts/codegen:daml2js",
)

alias(
    name = "daml2js@ghci",
    actual = "//language-support/ts/codegen:daml2js@ghci",
)

alias(
    name = "daml-lf-repl",
    actual = "//daml-lf/repl:repl",
)

alias(
    name = "bindings-java",
    actual = "//language-support/java/bindings:bindings-java",
)

alias(
    name = "yarn",
    actual = "@nodejs//:yarn",
)

alias(
    name = "java",
    actual = "@local_jdk//:bin/java.exe" if is_windows else "@local_jdk//:bin/java",
)

exports_files([
    ".scalafmt.conf",
])

# Buildifier.

load("@com_github_bazelbuild_buildtools//buildifier:def.bzl", "buildifier")

buildifier_excluded_patterns = [
    "./3rdparty/haskell/c2hs-package.bzl",
    "./3rdparty/haskell/network-package.bzl",
    "**/node_modules/*",
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
        "//compiler/daml-lf-ast:tests",
        "//compiler/damlc/daml-doc:daml-doc-testing",
        "//compiler/damlc/daml-ide-core:ide-testing",
        "//compiler/damlc/stable-packages:generate-stable-package",
        "//compiler/damlc/tests:daml-doctest",
        "//compiler/damlc/tests:damlc-test",
        "//compiler/damlc/tests:generate-simple-dalf",
        "//compiler/damlc/tests:incremental",
        "//compiler/damlc/tests:integration-dev",
        "//compiler/damlc/tests:packaging",
        "//daml-assistant:daml",
        "//daml-assistant:test",
        "//daml-assistant/daml-helper",
        "//daml-assistant/daml-helper:test-deployment",
        "//daml-assistant/daml-helper:test-tls",
        "//daml-assistant/integration-tests",
        "//language-support/hs/bindings:hs-ledger",
        "//language-support/hs/bindings:test",
        "//language-support/ts/codegen:daml2js",
    ],
)
