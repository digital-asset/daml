# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_darwin", "is_darwin_arm64", "is_linux", "is_windows")

# ghc-pkg is linked dynamically so to distribute it we have to throw it at
# package_app. However, the result of that is a tarball so if we try to add
# that to resources `bazel run` is not going to work. We thus use the
# dynamically linked executable in the runfiles of damlc and the tarball
# produced by package_app in the resources of damlc-dist.
#
_GHC_PKG_UNIX = "lib/bin/ghc-pkg"

def _ghc_pkg_label():
    if is_windows:
        return "@rules_haskell_ghc_windows_amd64//:bin/ghc-pkg.exe"
    elif is_linux:
        return "//bazel/haskell/toolchain:install_tree"
    elif is_darwin_arm64:
        return "@rules_haskell_ghc_darwin_arm64//:" + _GHC_PKG_UNIX
    elif is_darwin:
        return "@rules_haskell_ghc_darwin_amd64//:" + _GHC_PKG_UNIX
    else:
        fail("Unsupported platform for ghc-pkg")

ghc_pkg = _ghc_pkg_label()

def _ghc_pkg_dist_label():
    if is_linux:
        return "//bazel/haskell/toolchain:ghc_pkg_bin"
    return _ghc_pkg_label()

ghc_pkg_dist = _ghc_pkg_dist_label()
