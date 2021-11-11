# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# The dependencies of the daml workspace.
# This allows using the daml workspace externally
# from another bazel workspace.
#
# For example, another Bazel project can depend on
# targets in the daml repository by doing:
# ---
# local_repository(
#   name = "com_github_digital_asset_daml",
#   path = "/path/to/daml"
# )
# load("@com_github_digital_asset_daml//:deps.bzl", "daml_deps")
# daml_deps()
# ---
#
# A 3rd-party consumer would also need to register relevant
# toolchains and repositories in order to build targets.
# That is, copy some setup calls from WORKSPACE into the
# other WORKSPACE.
#
# Make sure to reference repository local files with the full
# prefix: @com_github_digital_asset_daml//..., as these won't
# be resolvable from external workspaces otherwise.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load(
    "@daml//:deps.bzl",
    "buildifier_sha256",
    "buildifier_version",
    "rules_bazel_common_sha256",
    "rules_bazel_common_version",
    "rules_haskell_patches",
    "rules_haskell_sha256",
    "rules_haskell_version",
    "rules_jvm_external_sha256",
    "rules_jvm_external_version",
    "rules_nixpkgs_patches",
    "rules_nixpkgs_sha256",
    "rules_nixpkgs_version",
    "rules_nodejs_sha256",
    "rules_nodejs_version",
    "rules_scala_sha256",
    "rules_scala_version",
    "zlib_sha256",
    "zlib_version",
)
load("//:versions.bzl", "latest_stable_version", "version_sha256s")
load("@os_info//:os_info.bzl", "os_name")

def daml_deps():
    if "rules_haskell" not in native.existing_rules():
        http_archive(
            name = "rules_haskell",
            strip_prefix = "rules_haskell-%s" % rules_haskell_version,
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            patches = [
                p.replace("@com_github_digital_asset_daml", "@daml")
                for p in rules_haskell_patches
            ],
            patch_args = ["-p1"],
            sha256 = rules_haskell_sha256,
        )

    if "io_tweag_rules_nixpkgs" not in native.existing_rules():
        http_archive(
            name = "io_tweag_rules_nixpkgs",
            strip_prefix = "rules_nixpkgs-%s" % rules_nixpkgs_version,
            urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
            sha256 = rules_nixpkgs_sha256,
            patches = [
                p.replace("@com_github_digital_asset_daml", "@daml")
                for p in rules_nixpkgs_patches
            ],
            patch_args = ["-p1"],
        )

    if "com_github_bazelbuild_buildtools" not in native.existing_rules():
        http_archive(
            name = "com_github_bazelbuild_buildtools",
            sha256 = buildifier_sha256,
            strip_prefix = "buildtools-{}".format(buildifier_version),
            url = "https://github.com/bazelbuild/buildtools/archive/{}.tar.gz".format(buildifier_version),
        )

    if "com_github_madler_zlib" not in native.existing_rules():
        http_archive(
            name = "com_github_madler_zlib",
            build_file = "@daml//3rdparty/c:zlib.BUILD",
            strip_prefix = "zlib-{}".format(zlib_version),
            urls = ["https://github.com/madler/zlib/archive/v{}.tar.gz".format(zlib_version)],
            sha256 = zlib_sha256,
        )

    if "build_bazel_rules_nodejs" not in native.existing_rules():
        http_archive(
            name = "build_bazel_rules_nodejs",
            urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/{}/rules_nodejs-{}.tar.gz".format(rules_nodejs_version, rules_nodejs_version)],
            sha256 = rules_nodejs_sha256,
            patches = [
                # Work around for https://github.com/bazelbuild/rules_nodejs/issues/1565
                "@daml//bazel_tools:rules_nodejs_npm_cli_path.patch",
                # Enforces a dependency of the rules_nodejs workspace on the
                # workspace providing node.
                "@daml//bazel_tools:rules_nodejs_node_dependency.patch",
            ],
            patch_args = ["-p1"],
        )

    if "rules_jvm_external" not in native.existing_rules():
        http_archive(
            name = "rules_jvm_external",
            strip_prefix = "rules_jvm_external-{}".format(rules_jvm_external_version),
            sha256 = rules_jvm_external_sha256,
            url = "https://github.com/bazelbuild/rules_jvm_external/archive/{}.zip".format(rules_jvm_external_version),
        )

    if "io_bazel_rules_scala" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_scala",
            url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
            type = "zip",
            strip_prefix = "rules_scala-%s" % rules_scala_version,
            sha256 = rules_scala_sha256,
            patches = [
                "@daml//bazel_tools:scala-escape-jvmflags.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_github_google_bazel_common" not in native.existing_rules():
        http_archive(
            name = "com_github_google_bazel_common",
            sha256 = rules_bazel_common_sha256,
            strip_prefix = "bazel-common-{}".format(rules_bazel_common_version),
            urls = ["https://github.com/google/bazel-common/archive/{}.zip".format(rules_bazel_common_version)],
        )

    # The tests for the `platform-version` field need a proper assistant installation with
    # multiple installed SDKs. Therefore, we fetch the (unextracted) installation
    # tarball for the latest stable release. Together with the tarball for HEAD
    # that gives us two SDK version to test with.
    http_file(
        name = "daml-sdk-tarball-latest-stable",
        sha256 = version_sha256s[latest_stable_version][os_name],
        urls = ["https://github.com/digital-asset/daml/releases/download/v{}/daml-sdk-{}-{}.tar.gz".format(latest_stable_version, latest_stable_version, os_name)],
    )
