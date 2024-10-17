# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# read_common_deps_json.bzl

def _read_common_deps_json_impl(ctx):
    json_file = ctx.path(ctx.attr.json_file)
    json_content = ctx.read(json_file)
    parsed_json = json.decode(json_content)

    # Write the parsed JSON content to a file
    parsed_json_file = ctx.path(ctx.attr.output_file)
    print(parsed_json_file)
    ctx.file("BUILD", "")
    ctx.file(parsed_json_file, "DEPS = " + str(parsed_json))

read_common_deps_json = repository_rule(
    implementation = _read_common_deps_json_impl,
    attrs = {
        "json_file": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The JSON file to read and parse",
        ),
        "output_file": attr.string(
            mandatory = True,
            doc = "The output bazel file that defines a DEPS constant",
        ),
    },
    doc = "A repository rule to read and parse a JSON file",
)
