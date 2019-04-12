# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "@io_tweag_rules_haskell//haskell:haskell.bzl",
    "haskell_binary",
    "haskell_library",
    "haskell_repl",
    "haskell_test",
)
load(
    "@io_tweag_rules_haskell//haskell:c2hs.bzl",
    "c2hs_library",
)
load(
    "@ai_formation_hazel//:hazel.bzl",
    "hazel_library",
)
load("//bazel_tools:hlint.bzl", "haskell_hlint")
load("@os_info//:os_info.bzl", "is_windows")

# This file defines common Haskell language extensions and compiler flags used
# throughout this repository. The initial set of flags is taken from the
# daml-foundations project. If you find that additional flags are required for
# another project, consider whether all projects could benefit from these
# changes. If so, add them here.
#
# Use the macros `da_haskell_*` defined in this file, instead of stock rules
# `haskell_*` from `rules_haskell` in order for these default flags to take
# effect.

common_haskell_exts = [
    "BangPatterns",
    "DeriveDataTypeable",
    "DeriveFoldable",
    "DeriveFunctor",
    "DeriveGeneric",
    "DeriveTraversable",
    "FlexibleContexts",
    "GeneralizedNewtypeDeriving",
    "LambdaCase",
    "NamedFieldPuns",
    "NumericUnderscores",
    "PackageImports",
    "RecordWildCards",
    "ScopedTypeVariables",
    "StandaloneDeriving",
    "TupleSections",
    "TypeApplications",
    "ViewPatterns",
]

common_haskell_flags = [
    "-Wall",
    "-Werror",
    "-Wincomplete-uni-patterns",
    "-Wno-name-shadowing",
    "-fno-omit-yields",
    "-threaded",
    "-rtsopts",

    # run on two cores, disable idle & parallel GC
    "-with-rtsopts=-N2 -qg -I0",
]

def _wrap_rule(rule, name = "", deps = [], hazel_deps = [], compiler_flags = [], **kwargs):
    ext_flags = ["-X%s" % ext for ext in common_haskell_exts]
    hazel_libs = [hazel_library(dep) for dep in hazel_deps]
    rule(
        name = name,
        compiler_flags = ext_flags + common_haskell_flags + compiler_flags,
        deps = hazel_libs + deps,
        **kwargs
    )

    # We don't fetch HLint on Windows
    if not is_windows:
        haskell_hlint(
            name = name + "@hlint",
            deps = [name],
            testonly = True,
        )

    # Load Main module on executable rules.
    repl_ghci_commands = []
    if "main_function" in kwargs:
        main_module = ".".join(kwargs["main_function"].split(".")[:-1])
        repl_ghci_commands = [
            ":m " + main_module,
        ]
    haskell_repl(
        name = name + "@ghci",
        deps = [name],
        experimental_from_binary = ["//nix/..."],
        repl_ghci_args = [
            "-fexternal-interpreter",
            "-j",
            "+RTS",
            "-I0",
            "-n2m",
            "-A128m",
            "-qb0",
            "-RTS",
        ],
        repl_ghci_commands = repl_ghci_commands,
        testonly = kwargs.get("testonly", False),
        visibility = ["//visibility:public"],
        # Whether to make runfiles, such as the daml stdlib, available to the
        # REPL. Pass --define ghci_data=True to enable.
        collect_data = select({
            "//:ghci_data": True,
            "//conditions:default": False,
        }),
    )

def da_haskell_library(**kwargs):
    """
    Define a Haskell library.

    Allows to define Hazel dependencies using `hazel_deps`,
    applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_library` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_library(
            name = "example",
            src_strip_prefix = "src",
            srcs = glob(["src/**/*.hs"]),
            hazel_deps = [
                "base",
                "text",
            ],
            deps = [
                "//some/package:target",
            ],
            visibility = ["//visibility:public"],
        )
        ```
    """
    _wrap_rule(haskell_library, **kwargs)

def da_haskell_binary(main_function = "Main.main", **kwargs):
    """
    Define a Haskell executable.

    Allows to define Hazel dependencies using `hazel_deps`,
    applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_binary` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_binary(
            name = "example",
            src_strip_prefix = "src",
            srcs = glob(["src/**/*.hs"]),
            hazel_deps = [
                "base",
                "text",
            ],
            deps = [
                "//some/package:target",
            ],
            visibility = ["//visibility:public"],
        )
        ```
    """
    _wrap_rule(
        haskell_binary,
        # Make this argument explicit, so we can distinguish library and
        # executable rules in _wrap_rule.
        main_function = main_function,
        **kwargs
    )

def da_haskell_test(main_function = "Main.main", testonly = True, **kwargs):
    """
    Define a Haskell test suite.

    Allows to define Hazel dependencies using `hazel_deps`,
    applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_test` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_test(
            name = "example",
            src_strip_prefix = "src",
            srcs = glob(["src/**/*.hs"]),
            hazel_deps = [
                "base",
                "text",
            ],
            deps = [
                "//some/package:target",
            ],
            visibility = ["//visibility:public"],
        )
        ```
    """
    _wrap_rule(
        haskell_test,
        # Make this argument explicit, so we can distinguish library and
        # executable rules in _wrap_rule.
        main_function = main_function,
        # Make this argument explicit, so we can distinguish test targets
        # in _wrap_rule.
        testonly = testonly,
        **kwargs
    )

def da_haskell_repl(**kwargs):
    """
    Define a Haskell repl.

    Applies common Haskell options defined in `bazel_tools/haskell.bzl`
    and forwards to `haskell_repl` from `rules_haskell`.
    Refer to the [`rules_haskell` documentation][rules_haskell_docs].

    [rules_haskell_docs]: https://api.haskell.build/

    Example:
        ```
        da_haskell_repl(
            name = "repl",
            # Load these packages and their dependencies by source.
            # Will only load packages from the local workspace by source.
            deps = [
                "//some/package:target_a",
                "//some/package:target_b",
            ],
            # (optional) only load targets matching these patterns by source.
            experimental_from_source = [
                "//some/package/...",
                "//some/other/package/...",
            ],
            # (optional) don't load targets matching these patterns by source.
            experimental_from_binary = [
                "//some/vendored/package/...",
            ],
            # (optional) Pass extra arguments to ghci.
            repl_ghci_args = [
                "-fexternal-interpreter",
            ],
            # (optional) Make runfiles available to the REPL, or not.
            collect_data = True,
        )
        ```

    """

    # The common_haskell_exts and common_haskell_flags are already passed on
    # the library/binary/test rule wrappers. haskell_repl will pick these up
    # automatically, if such targets are loaded by source.
    # Right now we don't set any default flags.
    haskell_repl(**kwargs)

def _sanitize_string_for_usage(s):
    res_array = []
    for idx in range(len(s)):
        c = s[idx]
        if c.isalnum() or c == ".":
            res_array.append(c)
        else:
            res_array.append("_")
    return "".join(res_array)

def c2hs_suite(name, hazel_deps, deps = [], srcs = [], c2hs_srcs = [], c2hs_src_strip_prefix = "", **kwargs):
    ts = []
    for file in c2hs_srcs:
        n = _sanitize_string_for_usage(file)
        c2hs_library(
            name = n,
            srcs = [file],
            deps = deps + [":" + t for t in ts],
            src_strip_prefix = c2hs_src_strip_prefix,
        )
        ts.append(n)
    da_haskell_library(
        name = name,
        srcs = [":" + t for t in ts] + srcs,
        deps = deps,
        hazel_deps = hazel_deps,
        **kwargs
    )

# Add extra packages, e.g., packages that are on Hackage but not in Stackage.
# This cannot be inlined since it is impossible to create a struct in WORKSPACE.
def add_extra_packages(pkgs, extra):
    return pkgs + {k: struct(**v) for (k, v) in extra}
