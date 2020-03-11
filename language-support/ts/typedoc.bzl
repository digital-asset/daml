# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")

def ts_docs(pkg_name):
    "Macro for Typescript documentation generation with typedoc"

    native.genrule(
        name = "docs",
        srcs = native.glob(["**/*.ts"], exclude = ["**/*test.ts"]) + [":README.md"],
        tools = ["@language_support_ts_deps//typedoc/bin:typedoc"],
        outs = [pkg_name + "-docs.tar.gz"],
        cmd = """
          # NOTE: we need the --ignoreCompilerErrors flag because we get errors when tsc is trying to
          # resolve the imported packages.
          $(location @language_support_ts_deps//typedoc/bin:typedoc) --out docs --ignoreCompilerErrors --readme README.md $(SRCS)
          tar czf $@ docs
        """,
        visibility = ["//visibility:public"],
    ) if not is_windows else None
