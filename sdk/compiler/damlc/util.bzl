# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")

# This is a gross hack: ghc-pkg is linked dynamically so to distribute
# it we have to throw it at package_app. However, the result of that
# is a tarball so if we try to add that to resources `bazel run` is
# not going to work. We thus use the dynamically linked executable in the runfiles of damlc
# and the tarball produced by package_app in the resources of damlc-dist.
ghc_pkg = "@rules_haskell_ghc_windows_amd64//:bin/ghc-pkg.exe" if is_windows else "@ghc_nix//:lib/ghc-9.0.2/bin/ghc-pkg"
