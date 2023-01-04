# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    """Load all Haskell dependencies of the Daml repository."""

    stack_snapshot(
        name = "stackage",
        extra_deps = {
            "zlib": ["@com_github_madler_zlib//:libz"],
        },
        stack_snapshot_json =
            "//:stackage_snapshot_windows.json" if is_windows else "//:stackage_snapshot.json",
        haddock = False,
        local_snapshot = "//:stack-snapshot.yaml",
        packages = [
            "aeson",
            "aeson-extra",
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
            "lens-aeson",
            "memory",
            "monad-loops",
            "mtl",
            "network",
            "optparse-applicative",
            "process",
            "safe",
            "safe-exceptions",
            "semver",
            "split",
            "stm",
            "tagged",
            "tar-conduit",
            "tasty",
            "tasty-hunit",
            "text",
            "typed-process",
            "unix-compat",
            "unordered-containers",
            "utf8-string",
            "uuid",
        ],
        components = {
            "attoparsec": [
                "lib:attoparsec",
                "lib:attoparsec-internal",
            ],
        },
        components_dependencies = {
            "attoparsec": """{"lib:attoparsec": ["lib:attoparsec-internal"]}""",
        },
        stack = "@stack_windows//:stack.exe" if is_windows else None,
    )

    if is_windows:
        native.new_local_repository(
            name = "stack_windows",
            build_file_content = """
exports_files(["stack.exe"], visibility = ["//visibility:public"])
""",
            path = dadew_tool_home("stack"),
        )
