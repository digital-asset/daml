# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@npm_bazel_typescript//:index.bzl", "ts_project")

def _da_ts_library_impl(ctx):
    return [
        DefaultInfo(
            files = depset(ctx.files.srcs),
            runfiles = ctx.runfiles(files = ctx.files.srcs),
        ),
    ]

# TODO This doesn’t quite work, we need a DeclarationInfo provider.
# ts_project has this but it doesn’t have module_name and module_root.
_da_ts_library_rule = rule(
    _da_ts_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "deps": attr.label_list(allow_files = True),
        "module_name": attr.string(),
        "module_root": attr.string(),
    },
)

def da_ts_library(
        name,
        tsconfig = "tsconfig.json",
        srcs = [],
        deps = [],
        module_name = "",
        module_root = "",
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
      module_name: The import name of this library. E.g. @daml/types.
      module_root: Treat sources as rooted under module_name.
    """
    outs = [
        s.replace(".ts", ext)
        for ext in [".js", ".d.ts"]
        for s in srcs
    ]
    ts_project(
        name = "_{}_tsc".format(name),
        srcs = srcs,
        tsconfig = tsconfig,
        deps = deps,
        tsc = "@language_support_ts_deps//typescript/bin:tsc",
        validate = False,
        **kwargs,
    )
    _da_ts_library_rule(
        name = name,
        srcs = outs,
        module_name = module_name,
        module_root = module_root,
        **kwargs
    )
