# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/cpp:lib_cc_configure.bzl", "get_cpu_value")

_dev_env_package_build_template = """
# DO NOT EDIT: automatically generated BUILD file for dev_env_package.bzl: {rule_name}
package(default_visibility = ["//visibility:public"])

all_files = glob(["**"])

exports_files(srcs=all_files)
"""

def _dev_env_package_impl(ctx):
    if get_cpu_value(ctx) == "x64_windows":
        ps = ctx.which("powershell")
        ps_result = ctx.execute([ps, "-Command", "dadew enable; dadew where"], quiet = True)
        if ps_result.return_code != 0:
            fail("Failed to obtain dadew location.\n Exit code %d.\n%s\n%s" %
                 (ps_result.return_code, ps_result.stdout, ps_result.stderr))

        dadew = ps_result.stdout.splitlines(keepends = False)[0]
        tool_home = "%s\\scoop\\apps\\%s\\current" % (dadew, ctx.attr.win_tool)
    else:
        tool_home = "../%s" % ctx.attr.nix_label.name

    python = ctx.which("python3")
    if python == None:
        python = ctx.which("python")
    if python == None:
        fail("Cannot find python3 executable")
    list_contents = ctx.path(Label("//bazel_tools/dev_env_package:dev_env_package.py"))
    res = ctx.execute([python, list_contents, tool_home])
    if res.return_code != 0:
        fail("Failed to list contents\nstdout:{}\nstderr:{}\n".format(res.stdout, res.stderr))
    for item in res.stdout.splitlines():
        src = tool_home + "/" + item
        dst = ctx.path(ctx.attr.symlink_path + "/" + item)
        ctx.symlink(src, dst)

    build_path = ctx.path("BUILD")
    build_content = _dev_env_package_build_template.format(
        rule_name = ctx.name,
    )
    ctx.file(build_path, content = build_content, executable = False)

dev_env_package = repository_rule(
    implementation = _dev_env_package_impl,
    attrs = {
        "nix_label": attr.label(
            mandatory = True,
        ),
        "win_tool": attr.string(
            doc = "Windows dev env tool name",
            mandatory = True,
        ),
        "symlink_path": attr.string(
            default = "",
            doc = "Optional symlink path, if a tool should be symlinked at a custom path in the package",
            mandatory = False,
        ),
    },
)
