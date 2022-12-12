# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")
load("@build_environment//:configuration.bzl", "sdk_version")
load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

def ts_docs(pkg_name, srcs, deps):
    "Macro for Typescript documentation generation with typedoc"

    npm_package_bin(
        name = "docs-raw",
        data = [":tsconfig.json"] + srcs + [":README.md"] + deps,
        tool =
            "@language_support_ts_deps//typedoc/bin:typedoc",
        output_dir = True,
        args = ["--tsconfig", "$(execpath :tsconfig.json)", "$(execpath :index.ts)", "--out", "$(@D)"],
        visibility = ["//visibility:public"],
    ) if not is_windows else None

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
    ) if not is_windows else None
