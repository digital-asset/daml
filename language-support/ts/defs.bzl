# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@language_support_ts_deps//typescript:index.bzl", "tsc")

def _ts_commonjs_library_impl(ctx):
    return [
        DefaultInfo(
            files = depset(ctx.files.srcs),
            runfiles = ctx.runfiles(files = ctx.files.srcs),
        ),
    ]

_ts_commonjs_library_rule = rule(
    _ts_commonjs_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "module_name": attr.string(),
        "module_root": attr.string(),
    },
)

def ts_commonjs_library(
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
    tsc(
        name = "_%s_commonjs" % name,
        data = [tsconfig] + srcs + deps,
        outs = outs,
        args = [
            "--outDir",
            "$(RULEDIR)",
            "--project",
            "$(location %s)" % tsconfig,
            "--declaration",
        ],
        **kwargs
    )
    # rules_nodejs does import remapping based on the module_name attribute.
    # The tsc macro is an instance of npm_package_bin under the covers which
    # doesn't take a module_name attribute. So, we use this wrapper rule to be
    # able to set the module_name attribute.
    _ts_commonjs_library_rule(
        name = name,
        srcs = outs,
        module_name = module_name,
        module_root = module_root,
        **kwargs
    )
