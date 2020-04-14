# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@build_bazel_rules_nodejs//:providers.bzl", "DeclarationInfo", "LinkablePackageInfo")
load("@npm_bazel_typescript//:index.bzl", "ts_project")

def _da_ts_library_impl(ctx):
    path = "/".join([p for p in [ctx.bin_dir.path, ctx.label.workspace_root, ctx.label.package] if p])
    print(path)
    print(ctx.attr.module_name)
    return [
        ctx.attr.dep[DefaultInfo],
        ctx.attr.dep[DeclarationInfo],
        LinkablePackageInfo(package_name = ctx.attr.module_name, path = path)]

# TODO This doesn’t quite work, we need a DeclarationInfo provider.
# ts_project has this but it doesn’t have module_name and module_root.
_da_ts_library_rule = rule(
    _da_ts_library_impl,
    attrs = {
        "dep": attr.label(),
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
        name = name + "_tsc",
        srcs = srcs,
        tsconfig = tsconfig,
        deps = deps,
        tsc = "@language_support_ts_deps//typescript/bin:tsc",
        validate = False,
        **kwargs,
    )
    _da_ts_library_rule(
        name = name,
        dep = name + "_tsc",
        module_name = module_name,
        module_root = module_root,
        **kwargs,
    )
