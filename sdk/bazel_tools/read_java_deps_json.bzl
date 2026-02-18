# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _read_java_deps_json_impl(ctx):
    json_file = ctx.path(ctx.attr.json_file)
    json_content = ctx.read(json_file)
    parsed_json = json.decode(json_content)
    parsed_json_file = ctx.path(ctx.attr.output_file)
    ctx.file("BUILD", "")
    ctx.file(parsed_json_file, "JAVA_DEPS = " + str(parsed_json))

read_java_deps_json = repository_rule(
    implementation = _read_java_deps_json_impl,
    attrs = {
        "json_file": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The JSON file to read and parse",
        ),
        "output_file": attr.string(
            mandatory = True,
            doc = "The output bazel file that defines a JAVA_DEPS constant",
        ),
    },
    doc = "A repository rule to read and parse a JSON file",
)
