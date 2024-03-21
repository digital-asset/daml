# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")

def _add_data_impl(ctx):
    executable = ctx.actions.declare_file(
        ctx.label.name + (".exe" if is_windows else ""),
    )
    ctx.actions.symlink(
        output = executable,
        target_file = ctx.executable.executable,
        is_executable = True,
    )

    runfiles = ctx.runfiles(files = [ctx.executable.executable] + ctx.files.data)
    runfiles = runfiles.merge(ctx.attr.executable[DefaultInfo].default_runfiles)
    for data_dep in ctx.attr.data:
        runfiles = runfiles.merge(data_dep[DefaultInfo].default_runfiles)

    return [DefaultInfo(
        executable = executable,
        files = depset(direct = [executable]),
        runfiles = runfiles,
    )]

add_data = rule(
    _add_data_impl,
    attrs = {
        "executable": attr.label(
            executable = True,
            cfg = "target",
            doc = "Create a symlink to this executable",
        ),
        "data": attr.label_list(
            allow_files = True,
            doc = "Add these data files to the executable's runfiles",
        ),
    },
    executable = True,
    doc = "Creates a new target for the given executable with additional runfiles.",
)
