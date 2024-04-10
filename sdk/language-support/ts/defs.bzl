# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@build_bazel_rules_nodejs//:index.bzl", "js_library")
load("@language_support_ts_deps//@bazel/typescript:index.bzl", "ts_project")
load("//language-support/ts:typedoc.bzl", "ts_docs")

def da_ts_library(
        name,
        tsconfig = "tsconfig.json",
        srcs = [],
        deps = [],
        module_name = "",
        source_map = True,
        declaration = True,
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
    """
    ts_project(
        name = "_%s_ts" % name,
        srcs = srcs,
        deps = deps,
        tsconfig = tsconfig,
        source_map = source_map,
        declaration = declaration,
    )
    js_library(
        name = name,
        package_name = module_name,
        deps = ["_%s_ts" % name],
        **kwargs
    )

    ts_docs(
        name,
        srcs,
        deps,
    )
