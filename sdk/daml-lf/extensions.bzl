# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _impl(ctx):
    json_path = ctx.path(ctx.attr.json_file)
    content = ctx.read(json_path)

    # Convert JSON null/true/false to Starlark None/True/False
    starlark_content = content \
        .replace("null", "None") \
        .replace("true", "True") \
        .replace("false", "False")

    ctx.file("BUILD.bazel", 'exports_files(["data.bzl"])')

    ctx.file("data.bzl", "DATA = " + starlark_content)

daml_versions_repo = repository_rule(
    implementation = _impl,
    attrs = {
        "json_file": attr.label(mandatory = True, allow_single_file = True),
    },
    local = True,  # Forces rebuild if the json file changes
)
