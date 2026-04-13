# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_darwin", "is_darwin_arm64", "is_linux_arm", "is_linux_intel", "is_windows")

# ghc-pkg is linked dynamically so to distribute it we have to throw it at
# package_app. However, the result of that is a tarball so if we try to add
# that to resources `bazel run` is not going to work. We thus use the
# dynamically linked executable in the runfiles of damlc and the tarball
# produced by package_app in the resources of damlc-dist.
#
# On Unix the real binary lives under lib/, not bin/ (bin/ghc-pkg is a wrapper
# script). The libdir differs between Linux and macOS:
#   Linux:   lib/bin/ghc-pkg       (libdir = lib)
#   macOS:   lib/lib/bin/ghc-pkg   (libdir = lib/lib)
_GHC_PKG_LINUX = "lib/bin/ghc-pkg"
_GHC_PKG_DARWIN = "lib/lib/bin/ghc-pkg"

def _ghc_pkg_label():
    if is_windows:
        return "@rules_haskell_ghc_windows_amd64//:bin/ghc-pkg.exe"
    elif is_linux_intel:
        return "@rules_haskell_ghc_linux_amd64//:" + _GHC_PKG_LINUX
    elif is_linux_arm:
        return "@rules_haskell_ghc_linux_arm64//:" + _GHC_PKG_LINUX
    elif is_darwin_arm64:
        return "@rules_haskell_ghc_darwin_arm64//:" + _GHC_PKG_DARWIN
    elif is_darwin:
        return "@rules_haskell_ghc_darwin_amd64//:" + _GHC_PKG_DARWIN
    else:
        fail("Unsupported platform for ghc-pkg")

ghc_pkg = _ghc_pkg_label()
