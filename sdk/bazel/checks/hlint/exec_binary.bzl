# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _exec_binary_impl(ctx):
    binary = ctx.attr.binary[DefaultInfo]
    return [DefaultInfo(
        files = binary.files,
        runfiles = ctx.runfiles(transitive_files = binary.files).merge(binary.default_runfiles),
    )]

exec_binary = rule(
    implementation = _exec_binary_impl,
    attrs = {
        "binary": attr.label(
            cfg = "exec",
            mandatory = True,
        ),
    },
)
