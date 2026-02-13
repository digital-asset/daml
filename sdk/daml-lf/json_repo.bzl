# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _impl(ctx):
    json_file = ctx.path(ctx.attr.json_file)
    json_content = ctx.read(json_file)
    parsed_json = json.decode(json_content)

    ctx.file("BUILD.bazel", 'exports_files(["data.bzl"])')

    ctx.file("data.bzl", "DATA = " + str(parsed_json))

daml_versions_repo = repository_rule(
    implementation = _impl,
    attrs = {
        "json_file": attr.label(mandatory = True, allow_single_file = True),
    },
    local = True,  # Forces rebuild if the json file changes
)
