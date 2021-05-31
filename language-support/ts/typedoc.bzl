# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")
load("@build_environment//:configuration.bzl", "sdk_version")

def ts_docs(pkg_name):
    "Macro for Typescript documentation generation with typedoc"

    native.genrule(
        name = "docs",
        srcs = native.glob(["**/*.ts"], exclude = ["**/*test.ts"]) + [":README.md"],
        tools = [
            "@language_support_ts_deps//typedoc/bin:typedoc",
            "//bazel_tools/sh:mktgz",
        ],
        outs = [pkg_name + "-docs.tar.gz"],
        cmd = """
          # NOTE: we need the --ignoreCompilerErrors flag because we get errors when tsc is trying to
          # resolve the imported packages.
          $(location @language_support_ts_deps//typedoc/bin:typedoc) --out docs --ignoreCompilerErrors --readme README.md --stripInternal $(SRCS)
          sed -i -e 's/0.0.0-SDKVERSION/{sdk_version}/' docs/**/*.html
          $(execpath //bazel_tools/sh:mktgz) $@ -h docs
        """,
        visibility = ["//visibility:public"],
    ) if not is_windows else None
