# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "patch", "workspace_and_buildfile")
load("@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl", "nixpkgs_package")
load("//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew_binary_bundle")
load("//nix:repositories.bzl", "common_nix_file_deps", "dev_env_nix_repos")
load(
    ":version.bzl",
    "GHC_LIB_PATCHES",
    "GHC_LIB_REPO_URL",
    "GHC_LIB_REV",
    "GHC_LIB_SHA256",
    "GHC_PATCHES",
    "GHC_REPO_URL",
    "GHC_REV",
)

def _ghc_lib_deps_windows():
    """Import dependencies of ghc-lib on Windows."""
    dadew_binary_bundle(
        name = "dadew_ghc_lib_deps",
        paths = [
            # msys2
            "msys2",
            "usr/bin/bash",
            # autoconf
            "usr/bin/autoconf",
            "usr/bin/autoconf-2.71",
            "usr/bin/autoheader",
            "usr/bin/autoheader-2.71",
            "usr/bin/autom4te",
            "usr/bin/autom4te-2.71",
            "usr/bin/autoreconf",
            "usr/bin/autoreconf-2.71",
            "usr/bin/autoscan",
            "usr/bin/autoscan-2.71",
            "usr/bin/autoupdate",
            "usr/bin/autoupdate-2.71",
            "usr/bin/ifnames",
            "usr/bin/ifnames-2.71",
            # automake
            "usr/bin/aclocal",
            "usr/bin/aclocal-1.16",
            "usr/bin/automake",
            "usr/bin/automake-1.16",
            # diffutils
            "usr/bin/cmp",
            "usr/bin/diff",
            "usr/bin/diff3",
            "usr/bin/sdiff",
            # git
            "usr/lib/git-core/git",
            # gnumake
            "usr/bin/make",
            # m4
            "usr/bin/m4",
            # ncurses
            "usr/bin/captoinfo",
            "usr/bin/clear",
            "usr/bin/infocmp",
            "usr/bin/infotocap",
            "usr/bin/reset",
            "usr/bin/tabs",
            "usr/bin/tic",
            "usr/bin/tput",
            "usr/bin/tset",
            # perl
            "usr/bin/perl",
            # xz
            "usr/bin/lzcat",
            "usr/bin/lzcmp",
            "usr/bin/lzdiff",
            "usr/bin/lzegrep",
            "usr/bin/lzfgrep",
            "usr/bin/lzgrep",
            "usr/bin/lzless",
            "usr/bin/lzma",
            "usr/bin/lzmadec",
            "usr/bin/lzmainfo",
            "usr/bin/lzmore",
            "usr/bin/unlzma",
            "usr/bin/unxz",
            "usr/bin/xz",
            "usr/bin/xzcat",
            "usr/bin/xzcmp",
            "usr/bin/xzdec",
            "usr/bin/xzdiff",
            "usr/bin/xzegrep",
            "usr/bin/xzfgrep",
            "usr/bin/xzgrep",
            "usr/bin/xzless",
            "usr/bin/xzmore",
        ],
        tool = "msys2",
    )

    dadew_binary_bundle(
        name = "dadew_ghc_lib_deps_python",
        paths = [
            "python:python3",
        ],
        tool = "python-3.8.2",
    )

    http_archive(
        name = "ghc_win",
        build_file_content = """\
load("@rules_sh//sh:sh.bzl", "sh_binaries")
sh_binaries(
    name = "tools",
    srcs = glob(["ghc-9.0.2*/bin/*"]),
    visibility = ["//visibility:public"],
)
""",
        sha256 = "f6fbb8047ae16049dc6215a6abb652b4307205310bfffddea695a854af92dc99",
        urls = ["https://downloads.haskell.org/~ghc/9.0.2/ghc-9.0.2-x86_64-unknown-mingw32.tar.xz"],
    )

    http_archive(
        name = "cabal_win",
        build_file_content = """\
load("@rules_sh//sh:sh.bzl", "sh_binaries")
sh_binaries(
    name = "tools",
    srcs = glob(["cabal.exe"]),
    visibility = ["//visibility:public"],
)
""",
        sha256 = "8222b49b6eac3d06aaa390bc688f467e8f949a38943567f46246f8320fd72ded",
        urls = ["https://downloads.haskell.org/~cabal/cabal-install-3.6.0.0/cabal-install-3.6.0.0-x86_64-windows.zip"],
    )

def _ghc_lib_deps_unix():
    """Import dependencies of ghc-lib on Linux and MacOS."""
    nixpkgs_package(
        name = "nix_ghc_lib_deps",
        attribute_path = "ghc_lib_deps",
        build_file = "@//bazel_tools/ghc-lib:BUILD.nix-deps",
        fail_not_supported = False,
        nix_file = "//nix:bazel.nix",
        nix_file_deps = common_nix_file_deps,
        repositories = dev_env_nix_repos,
    )

def ghc_lib_and_dependencies():
    """Import ghc-lib and its Bazel dependencies.

    Import the ghc-lib repository and the corresponding GHC repository as well
    as any dependencies needed to generate the ghc-lib(-parser) Cabal sdists.

    Note, this does not cover any Stackage dependencies, these are imported
    using `stack_snapshot`. The final `ghc-lib(-parser)` Haskell packages are
    also exposed via `@stackage` as vendored packages.
    """
    _ghc_lib_deps_unix()
    _ghc_lib_deps_windows()
    http_archive(
        name = "ghc-lib-gen",
        url = "{}/archive/{}.tar.gz".format(GHC_LIB_REPO_URL, GHC_LIB_REV),
        sha256 = GHC_LIB_SHA256,
        strip_prefix = "ghc-lib-{}".format(GHC_LIB_REV),
        build_file = "@//bazel_tools/ghc-lib:BUILD.ghc-lib-gen",
        patches = GHC_LIB_PATCHES,
        patch_args = ["-p1"],
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
