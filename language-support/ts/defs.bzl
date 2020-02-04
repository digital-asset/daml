# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@language_support_ts_deps//typescript:index.bzl", "tsc")

def ts_commonjs_library(
        name,
        tsconfig = "tsconfig.json",
        srcs = [],
        deps = [],
        **kwargs):
    """Build a typescript library.

    Invokes tsc and generates definitions and commonjs files.

    Attrs:
      name: A unique name for the rule.
      tsconfig: The tsconfig.json file.
        The "files" attribute defines the typescript sources.
      srcs: The typescript source files.
        Defines which files are visible to the typescript compiler.
      deps: Typescript library dependencies.
    """
    tsc(
        name = name,
        data = [tsconfig] + srcs + deps,
        outs = [
            s.replace(".ts", ext)
            for ext in [".js", ".d.ts"]
            for s in srcs
        ],
        args = [
            "--outDir",
            "$(RULEDIR)",
            "--project",
            "$(location %s)" % tsconfig,
            "--declaration",
        ],
    )
