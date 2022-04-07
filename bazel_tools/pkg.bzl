# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")

def pkg_empty_zip(name, out):
    native.genrule(
        name = name,
        srcs = [],
        outs = [out],
        # minimal empty zip file in Base64 encoding
        cmd = "echo UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA== | base64 -d > $@",
    )

def _unpack_tar_impl(ctx):
    outputs = []
    for out in ctx.attr.outs:
        if out.endswith("/"):
            outputs.append(ctx.actions.declare_directory(out[:-1]))
        else:
            outputs.append(ctx.actions.declare_file(out))
    src = ctx.file.src
    args = ctx.actions.args()
    if src.path.endswith(".tar.gz") or src.path.endswith(".tgz"):
        command = "gzip -cd {src} | tar x $@".format(
            src = src.path,
        )
    else:
        command = "tar xf {src} $@".format(
            src = src.path,
        )
    if ctx.attr.strip:
        args.add_all(["--strip", ctx.attr.strip])
    prefix = paths.join(
        ctx.bin_dir.path,
        ctx.label.workspace_root,
        ctx.label.package,
        ctx.attr.prefix,
    )
    args.add_all(["-C", prefix])
    ctx.actions.run_shell(
        outputs = outputs,
        inputs = [ctx.file.src],
        command = command,
        arguments = [args],
        mnemonic = "UnpackTar",
        progress_message = "Unpacking {} to {}".format(
            ctx.attr.src.label,
            prefix,
        ),
    )
    return [DefaultInfo(
        files = depset(direct = outputs),
    )]

unpack_tar = rule(
    _unpack_tar_impl,
    attrs = dict(
        src = attr.label(
            doc = "The archive to unpack.",
            # Add further extensions on demand.
            allow_single_file = [".tar", ".tar.gz", ".tgz"],
            mandatory = True,
        ),
        outs = attr.string_list(
            doc = "The outputs to capture. Mark directory outputs with a `/` suffix.",
        ),
        strip = attr.int(
            doc = "Strip this many leading components from file names on extraction.",
        ),
        prefix = attr.string(
            doc = "Add this prefix to the unpacked paths, relative to the current package.",
        ),
    ),
)
