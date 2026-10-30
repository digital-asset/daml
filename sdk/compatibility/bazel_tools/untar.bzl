# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _untar_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    tar = ctx.toolchains["@tar.bzl//tar/toolchain:type"].tarinfo
    args = ctx.actions.args()
    args.add("-x")
    args.add("-f", ctx.file.src)
    args.add("-C", out.path)
    args.add("--strip-components", str(ctx.attr.strip_components))
    ctx.actions.run(
        executable = tar.binary,
        arguments = [args],
        inputs = [ctx.file.src],
        outputs = [out],
        env = tar.default_env,
        mnemonic = "CompatUntar",
        progress_message = "Unpacking %{label}",
        execution_requirements = {"no-cache": "1"},
    )
    return [DefaultInfo(
        files = depset([out]),
        runfiles = ctx.runfiles(files = [out]),
    )]

untar = rule(
    implementation = _untar_impl,
    attrs = {
        "src": attr.label(allow_single_file = True, mandatory = True),
        "strip_components": attr.int(default = 0),
    },
    toolchains = ["@tar.bzl//tar/toolchain:type"],
)
