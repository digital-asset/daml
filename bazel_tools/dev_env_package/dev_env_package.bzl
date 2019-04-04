# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
        ps_result = ctx.execute([ps, "-Command", "dadew enable; dadew where"], quiet=True)
        if ps_result.return_code != 0:
            fail("Failed to obtain dadew location.\n Exit code %d.\n%s\n%s" %
            (ps_result.return_code, ps_result.stdout, ps_result.stderr))

        dadew = ps_result.stdout.splitlines(keepends=False)[0]
        tool_home = "%s\\scoop\\apps\\%s\\current" % (dadew, ctx.attr.win_tool)
    else:
        tool_home = "../%s" % ctx.attr.nix_label.name

    ctx.symlink(tool_home, ctx.path(ctx.attr.symlink_path))

    build_path = ctx.path("BUILD")
    build_content = _dev_env_package_build_template.format(
        rule_name = ctx.name
    )
    ctx.file(build_path, content=build_content, executable=False)

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
