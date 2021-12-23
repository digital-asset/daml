# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "patch", "workspace_and_buildfile")
load(
    ":version.bzl",
    "GHC_LIB_REPO_URL",
    "GHC_LIB_REV",
    "GHC_LIB_SHA256",
    "GHC_PATCHES",
    "GHC_REPO_URL",
    "GHC_REV",
    "GHC_SHA256",
)

def ghc_lib():
    http_archive(
        name = "ghc-lib-gen",
        url = "{}/archive/{}.tar.gz".format(GHC_LIB_REPO_URL, GHC_LIB_REV),
        sha256 = GHC_LIB_SHA256,
        strip_prefix = "ghc-lib-{}".format(GHC_LIB_REV),
        build_file = "@//bazel_tools/ghc-lib:BUILD.ghc-lib-gen",
    )
    new_git_repository(
        name = "da-ghc",
        remote = GHC_REPO_URL,
        commit = GHC_REV,
        recursive_init_submodules = True,
        build_file = "@//bazel_tools/ghc-lib:BUILD.ghc",
        shallow_since = "1639050525 +0100",
        patches = GHC_PATCHES,
        patch_args = ["-p1"],
    )
