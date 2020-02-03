# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

_sdk_version_bzl_template = """
sdk_version = "{SDK_VERSION}"
"""

def _sdk_version_impl(repository_ctx):
    version = repository_ctx.read(repository_ctx.attr.version).strip()
    sdk_version_substitutions = {
        "SDK_VERSION": version,
    }
    repository_ctx.file(
        "sdk_version.bzl",
        _sdk_version_bzl_template.format(**sdk_version_substitutions),
        False,
    )
    repository_ctx.file(
        "BUILD",
        "",
        False,
    )

sdk_version = repository_rule(
    implementation = _sdk_version_impl,
    attrs = {"version": attr.label(default = "//:VERSION")},
)
