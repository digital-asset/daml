# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@aspect_bazel_lib//lib:directory_path.bzl", "directory_path")
load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_run_binary")
load("@build_environment//:configuration.bzl", "sdk_version")

def ts_docs(pkg_name, srcs, deps):
    """Macro for Typescript documentation generation with typedoc.

    Requires npm_link_all_packages(name = "node_modules") in the calling BUILD file.
    """
    directory_path(
        name = "_typedoc_entrypoint",
        directory = ":node_modules/typedoc/dir",
        path = "dist/lib/cli.js",
    )

    js_binary(
        name = "_typedoc_bin",
        entry_point = ":_typedoc_entrypoint",
        data = [
            ":node_modules/typedoc",
            ":node_modules/typescript",
        ],
    )

    js_run_binary(
        name = "docs-raw",
        srcs = [":tsconfig.json"] + srcs + [":README.md"] + deps,
        tool = ":_typedoc_bin",
        chdir = native.package_name(),
        out_dirs = ["docs-raw"],
        args = [
            "--tsconfig",
            "tsconfig.json",
            "index.ts",
            "--out",
            "docs-raw",
        ],
        target_compatible_with = select({
            "@platforms//os:windows": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }),
    )

    native.genrule(
        name = "docs",
        tools = ["//bazel_tools/sh:mktgz"],
        outs = [pkg_name + "-docs.tar.gz"],
        srcs = [":docs-raw"],
        cmd = """
          set -eou pipefail
          DIR=$(location :docs-raw)
          WORKDIR=$$(mktemp -d)
          trap "rm -rf $$WORKDIR" EXIT
          mkdir -p $$WORKDIR/docs
          cp -r $$DIR/* $$WORKDIR/docs

          # Replace version number in all files
          sed -i -e 's/0.0.0-SDKVERSION/{sdk_version}/' $$WORKDIR/**/*.html

          # We want the NPM version of the docs (i.e. the README.md) to point
          # back to our own documentation, but here we're creating our local
          # copy and that one shouldn't link to itself.
          sed -i -e '/START_BACKLINK/,/END_BACKLINK/d' $$WORKDIR/docs/index.html

          OUT=$$PWD/$@
          MKTGZ=$$PWD/$(execpath //bazel_tools/sh:mktgz)
          cd $$WORKDIR
          $$MKTGZ $$OUT -h docs
        """.format(sdk_version = sdk_version),
        visibility = ["//visibility:public"],
        target_compatible_with = select({
            "@platforms//os:windows": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }),
    )
