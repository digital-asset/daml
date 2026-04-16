# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@aspect_bazel_lib//lib:directory_path.bzl", "directory_path")
load("@aspect_rules_js//js:defs.bzl", "js_test")

def eslint_test(name, srcs, tsconfig = ":tsconfig.json", eslintrc = ":.eslintrc.json", data = [], node_modules = ":node_modules", **kwargs):
    """Run eslint on the given TypeScript sources.

    Requires npm_link_all_packages(name = "node_modules") in the calling BUILD file.

    Args:
      name: The name of the generated test rule.
      srcs: The source files to lint.
      tsconfig: The tsconfig.json file (used for parser).
      eslintrc: The .eslintrc.json file.
      data: Additional runtime dependencies.
      node_modules: Label string pointing to the npm_link_all_packages output.
    """
    directory_path(
        name = "_%s_eslint_entrypoint" % name,
        directory = "%s/eslint/dir" % node_modules,
        path = "bin/eslint.js",
    )

    src_filenames = [src.split(":")[-1] if ":" in src else src for src in srcs]

    args = [
        "--max-warnings",
        "0",
    ] + src_filenames

    js_test(
        name = name,
        entry_point = ":_%s_eslint_entrypoint" % name,
        chdir = native.package_name(),
        data = srcs + [tsconfig, eslintrc] + data + [
            "%s/@typescript-eslint/eslint-plugin" % node_modules,
            "%s/@typescript-eslint/parser" % node_modules,
            "%s/eslint" % node_modules,
            "%s/typescript" % node_modules,
        ],
        args = args,
        target_compatible_with = select({
            "@platforms//os:windows": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }),
        **kwargs
    )
