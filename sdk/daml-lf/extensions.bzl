# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# extensions.bzl

def _json_loader_impl(ctx):
    # 1. Read the JSON file from the workspace
    # We use the label referring to the file in your repo
    content = ctx.read(ctx.attr.json_file)

    # 2. Convert JSON null/true/false to Starlark None/True/False
    starlark_content = content \
        .replace("null", "None") \
        .replace("true", "True") \
        .replace("false", "False")

    # 3. Create the build file so this repo is visible
    ctx.file("BUILD.bazel", 'exports_files(["data.bzl"])')

    # 4. Write the parsed data to a .bzl file
    ctx.file("data.bzl", "DATA = " + starlark_content)

# Define the repository rule
_json_loader_repo = repository_rule(
    implementation = _json_loader_impl,
    attrs = {
        "json_file": attr.label(mandatory = True, allow_single_file = True),
    },
    local = True, # Forces rebuild if the json file changes
)

def _ver_extension_impl(ctx):
    # Instantiate the repo rule defined above
    _json_loader_repo(
        name = "daml_versions_data",
        json_file = Label("//daml-lf:daml-lf-versions.json"), # Point this to your actual JSON file location
    )

# Export the extension for use in MODULE.bazel
version_extension = module_extension(
    implementation = _ver_extension_impl,
)

