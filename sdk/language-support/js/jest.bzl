# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@aspect_bazel_lib//lib:directory_path.bzl", "directory_path")
load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_test")

def jest_test(name, srcs, deps, jest_config = ":jest.config.js", tsconfig = ":tsconfig.json", node_modules = ":node_modules", **kwargs):
    """Macro for TypeScript test suites with jest.

    Requires npm_link_all_packages(name = "node_modules") in the calling BUILD file.

    Args:
      name: The name of the generated test rule.
      srcs: The test source files.
      deps: Dependencies (typically :node_modules/... targets).
      jest_config: The jest configuration file.
      tsconfig: The tsconfig.json file.
      node_modules: Label string pointing to the npm_link_all_packages output.
    """
    directory_path(
        name = "_%s_jest_entrypoint" % name,
        directory = "%s/jest-cli/dir" % node_modules,
        path = "bin/jest.js",
    )

    args = [
        "--no-cache",
        "--no-watchman",
        "--ci",
        "--config",
        "$(rootpath %s)" % jest_config,
    ]
    for src in srcs:
        args.extend(["--runTestsByPath", "$(rootpath %s)" % src])

    js_test(
        name = name,
        entry_point = ":_%s_jest_entrypoint" % name,
        data = [jest_config, tsconfig] + srcs + deps + [
            "%s/jest" % node_modules,
            "%s/jest-cli" % node_modules,
            "%s/ts-jest" % node_modules,
            "%s/typescript" % node_modules,
            "%s/@types/jest" % node_modules,
        ],
        args = args,
        target_compatible_with = select({
            "@platforms//os:windows": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }),
        **kwargs
    )
