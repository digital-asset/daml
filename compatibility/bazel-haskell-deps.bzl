# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Defines external Haskell dependencies.
#
# Add Stackage dependencies to the `packages` attribute of the `@stackage`
# `stack_snapshot` in the very bottom of this file. If a package or version is
# not available on Stackage, add it to the custom stack snapshot in
# `stack-snapshot.yaml`. If a library requires patching, then add it as an
# `http_archive` and add it to the `vendored_packages` attribute of
# `stack_snapshot`. Executables are defined in an `http_archive` using
# `haskell_cabal_binary`.

load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@os_info//:os_info.bzl", "is_windows")
load("@rules_haskell//haskell:cabal.bzl", "stack_snapshot")

def daml_haskell_deps():
    """Load all Haskell dependencies of the DAML repository."""

    use_integer_simple = True

    stack_snapshot(
        name = "stackage",
        extra_deps = {
            "zlib": ["@com_github_madler_zlib//:libz"],
        },
        flags = dicts.add(
            {
                "integer-logarithms": ["-integer-gmp"],
                "text": ["integer-simple"],
                "scientific": ["integer-simple"],
            } if use_integer_simple else {},
        ),
        haddock = False,
        local_snapshot = "//:stack-snapshot.yaml",
        packages = [
            "base",
            "directory",
            "extra",
            "filepath",
            "monad-loops",
            "network",
            "process",
            "safe-exceptions",
            "split",
            "text",
        ] + (["unix"] if not is_windows else ["Win32"]),
        stack = None,
        tools = [
        ],
    )
