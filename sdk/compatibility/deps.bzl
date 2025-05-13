# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    "bazel_gazelle_sha256",
    "bazel_gazelle_version",
    "buildifier_sha256",
    "buildifier_version",
    "rules_bazel_common_sha256",
    "rules_bazel_common_version",
    "rules_go_sha256",
    "rules_go_version",
    "rules_haskell_patches",
    "rules_haskell_sha256",
    "rules_haskell_version",
    "rules_jvm_external_sha256",
    "rules_jvm_external_version",
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
    http_archive(
        name = "rules_java",
        urls = [
            "https://github.com/bazelbuild/rules_java/releases/download/7.3.1/rules_java-7.3.1.tar.gz",
        ],
        sha256 = "4018e97c93f97680f1650ffd2a7530245b864ac543fd24fae8c02ba447cb2864",
    )

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
        # N.B. rules_nixpkgs was split into separate components, which need to be loaded separately
        #
        # See https://github.com/tweag/rules_nixpkgs/issues/182 for the rational

        strip_prefix = "rules_nixpkgs-%s" % rules_nixpkgs_version

        http_archive(
            name = "io_tweag_rules_nixpkgs",
            strip_prefix = strip_prefix,
            urls = ["https://github.com/tweag/rules_nixpkgs/releases/download/v{}/rules_nixpkgs-{}.tar.gz".format(rules_nixpkgs_version, rules_nixpkgs_version)],
            sha256 = rules_nixpkgs_sha256,
            patch_args = ["-p1"],
        )

        http_archive(
            name = "rules_nixpkgs_core",
            strip_prefix = strip_prefix + "/core",
            urls = ["https://github.com/tweag/rules_nixpkgs/releases/download/v{}/rules_nixpkgs-{}.tar.gz".format(rules_nixpkgs_version, rules_nixpkgs_version)],
            sha256 = rules_nixpkgs_sha256,
            patch_args = ["-p2"],
        )

        http_archive(
            name = "rules_nixpkgs_nodejs",
            strip_prefix = strip_prefix + "/toolchains/nodejs",
            urls = ["https://github.com/tweag/rules_nixpkgs/releases/download/v{}/rules_nixpkgs-{}.tar.gz".format(rules_nixpkgs_version, rules_nixpkgs_version)],
            sha256 = rules_nixpkgs_sha256,
        )

        for toolchain in ["cc", "java", "python", "go", "rust", "posix"]:
            http_archive(
                name = "rules_nixpkgs_" + toolchain,
                strip_prefix = strip_prefix + "/toolchains/" + toolchain,
                urls = ["https://github.com/tweag/rules_nixpkgs/releases/download/v{}/rules_nixpkgs-{}.tar.gz".format(rules_nixpkgs_version, rules_nixpkgs_version)],
                sha256 = rules_nixpkgs_sha256,
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

    if "build_bazel_rules_apple" not in native.existing_rules():
        http_archive(
            name = "build_bazel_apple_support",
            sha256 = "73c8dc6cdd7cea87956db9279a69c9e68bd2a5ec6a6a507e36d6e2d7da4d71a4",
            url = "https://github.com/bazelbuild/apple_support/releases/download/1.21.1/apple_support.1.21.1.tar.gz",
        )
        http_archive(
            name = "build_bazel_rules_apple",
            sha256 = "73ad768dfe824c736d0a8a81521867b1fb7a822acda2ed265897c03de6ae6767",
            url = "https://github.com/bazelbuild/rules_apple/releases/download/3.20.1/rules_apple.3.20.1.tar.gz",
        )

    if "build_bazel_rules_nodejs" not in native.existing_rules():
        http_archive(
            name = "build_bazel_rules_nodejs",
            urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/{}/rules_nodejs-{}.tar.gz".format(rules_nodejs_version, rules_nodejs_version)],
            sha256 = rules_nodejs_sha256,
            patches = [
                "@daml//bazel_tools:rules_nodejs_hotfix.patch",
            ],
            patch_args = ["-p1"],
        )

    if "rules_jvm_external" not in native.existing_rules():
        http_archive(
            name = "rules_jvm_external",
            strip_prefix = "rules_jvm_external-{}".format(rules_jvm_external_version),
            sha256 = rules_jvm_external_sha256,
            url = "https://github.com/bazel-contrib/rules_jvm_external/releases/download/%s/rules_jvm_external-%s.tar.gz" % (rules_jvm_external_version, rules_jvm_external_version),
        )

    if "io_bazel_rules_scala" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_scala",
            url = "https://github.com/bazelbuild/rules_scala/releases/download/v{}/rules_scala-v{}.tar.gz".format(rules_scala_version, rules_scala_version),
            strip_prefix = "rules_scala-%s" % rules_scala_version,
            sha256 = rules_scala_sha256,
            patches = [
                "@daml//bazel_tools:scala-escape-jvmflags.patch",
            ],
            patch_args = ["-p1"],
        )

    if "bazel_gazelle" not in native.existing_rules():
        http_archive(
            name = "bazel_gazelle",
            urls = [
                "https://github.com/bazel-contrib/bazel-gazelle/releases/download/v{}/bazel-gazelle-v{}.tar.gz".format(bazel_gazelle_version, bazel_gazelle_version),
            ],
            sha256 = bazel_gazelle_sha256,
        )

    if "io_bazel_rules_go" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_go",
            urls = [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v{version}/rules_go-v{version}.zip".format(version = rules_go_version),
                "https://github.com/bazelbuild/rules_go/releases/download/v{version}/rules_go-v{version}.zip".format(version = rules_go_version),
            ],
            sha256 = rules_go_sha256,
        )

    if "com_github_google_bazel_common" not in native.existing_rules():
        http_archive(
            name = "com_github_google_bazel_common",
            sha256 = rules_bazel_common_sha256,
            strip_prefix = "bazel-common-{}".format(rules_bazel_common_version),
            urls = ["https://github.com/google/bazel-common/releases/download/{}/bazel-common-{}.tar.gz".format(rules_bazel_common_version, rules_bazel_common_version)],
        )
