# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
load("@daml//:deps.bzl", "buildifier_sha256", "buildifier_version", "rules_haskell_sha256", "rules_haskell_version", "rules_nixpkgs_sha256", "rules_nixpkgs_version")

def daml_deps():
    if "rules_haskell" not in native.existing_rules():
        http_archive(
            name = "rules_haskell",
            strip_prefix = "rules_haskell-%s" % rules_haskell_version,
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            patches = [
                "@daml//bazel_tools:haskell-strict-source-names.patch",
                "@daml//bazel_tools:haskell-windows-remove-fake-libs.patch",
                "@daml//bazel_tools:haskell-windows-extra-libraries.patch",
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
        )

    if "com_github_bazelbuild_buildtools" not in native.existing_rules():
        http_archive(
            name = "com_github_bazelbuild_buildtools",
            sha256 = buildifier_sha256,
            strip_prefix = "buildtools-{}".format(buildifier_version),
            url = "https://github.com/bazelbuild/buildtools/archive/{}.tar.gz".format(buildifier_version),
        )
