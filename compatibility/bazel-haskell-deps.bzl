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
load("@dadew//:dadew.bzl", "dadew_tool_home")

def daml_haskell_deps():
    """Load all Haskell dependencies of the DAML repository."""

    use_integer_simple = not is_windows

    stack_snapshot(
        name = "stackage",
        extra_deps = {
            "zlib": ["@com_github_madler_zlib//:libz"],
        },
        flags = dicts.add(
            {
                "cryptonite": ["-integer-gmp"],
                "hashable": ["-integer-gmp"],
                "integer-logarithms": ["-integer-gmp"],
                "text": ["integer-simple"],
                "scientific": ["integer-simple"],
            } if use_integer_simple else {},
        ),
        haddock = False,
        local_snapshot = "//:stack-snapshot.yaml",
        packages = [
            "aeson",
            "async",
            "base",
            "bytestring",
            "conduit",
            "conduit-extra",
            "containers",
            "cryptonite",
            "directory",
            "extra",
            "filepath",
            "http-client",
            "http-conduit",
            "jwt",
            "lens",
            "memory",
            "mtl",
            "monad-loops",
            "network",
            "optparse-applicative",
            "process",
            "safe",
            "safe-exceptions",
            "semver",
            "split",
            "tagged",
            "tar-conduit",
            "tasty",
            "tasty-hunit",
            "text",
            "optparse-applicative",
            "unix-compat",
            "unordered-containers",
        ] + (["unix"] if not is_windows else ["Win32"]),
        stack = "@stack_windows//:stack.exe" if is_windows else None,
        tools = [
        ],
    )

    if is_windows:
        native.new_local_repository(
            name = "stack_windows",
            build_file_content = """
exports_files(["stack.exe"], visibility = ["//visibility:public"])
""",
            path = dadew_tool_home("stack"),
        )
