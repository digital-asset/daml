# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@aspect_rules_js//js:defs.bzl", "js_library")
load("@aspect_rules_ts//ts:defs.bzl", "ts_project")
load("//language-support/js:typedoc.bzl", "ts_docs")

def da_ts_library(
        name,
        tsconfig = "tsconfig.json",
        srcs = [],
        deps = [],
        module_name = "",
        source_map = True,
        declaration = True,
        out_dir = None,
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
        No longer used for js_library package_name (rules_js does not support it);
        kept for API compatibility. The package identity is set on npm_package instead.
      out_dir: Must match outDir in tsconfig.json if set.
    """
    ts_project_kwargs = {
        "name": "_%s_ts" % name,
        "srcs": srcs,
        "deps": deps,
        "tsconfig": tsconfig,
        "transpiler": "tsc",
        "source_map": source_map,
        "declaration": declaration,
    }
    if out_dir:
        ts_project_kwargs["out_dir"] = out_dir
    ts_project(**ts_project_kwargs)
    js_library(
        name = name,
        srcs = ["_%s_ts" % name],
        **kwargs
    )

    ts_docs(
        name,
        srcs,
        deps,
    )
