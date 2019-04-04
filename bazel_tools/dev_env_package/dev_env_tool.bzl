# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/cpp:lib_cc_configure.bzl", "get_cpu_value")

_dev_env_tool_build_template = """
# DO NOT EDIT: automatically generated BUILD file for dev_env_package.bzl: {rule_name}
package(default_visibility = ["//visibility:public"])

all_files = glob(["**"])

filegroup(
    name = "{tool}",
    srcs = select({lbrace}
        ":windows": ["{win_path}"],
        "//conditions:default": ["{nix_path}"],
    {rbrace}),
)

config_setting(
    name = "windows",
    values = {lbrace}"cpu": "x64_windows"{rbrace},
    visibility = ["//visibility:private"],
)
"""

def _dev_env_tool_impl(ctx):
    if get_cpu_value(ctx) == "x64_windows":
        ps = ctx.which("powershell")
        ps_result = ctx.execute([ps, "-Command", "dadew enable; dadew where"], quiet=True)
        if ps_result.return_code != 0:
            fail("Failed to obtain dadew location.\n Exit code %d.\n%s\n%s" %
            (ps_result.return_code, ps_result.stdout, ps_result.stderr))

        dadew = ps_result.stdout.splitlines(keepends=False)[0]
        tool_home = "%s\\scoop\\apps\\%s\\current" % (dadew, ctx.attr.win_tool)
        for i in ctx.attr.win_include:
            ctx.symlink("%s\\%s"%(tool_home, i), "%s\\%s"%(ctx.path(""), i))

    else:
        tool_home = "../%s" % ctx.attr.nix_label.name
        for i in ctx.attr.nix_include:
            ctx.symlink("%s/%s"%(tool_home, i), "%s/%s"%(ctx.path(""), i))

    build_path = ctx.path("BUILD")
    build_content = _dev_env_tool_build_template.format(
        rule_name = ctx.name,
        tool = ctx.attr.tool,
        win_path = ctx.attr.win_path,
        nix_path = ctx.attr.nix_path,
        lbrace = "{",
        rbrace = "}",
    )
    ctx.file(build_path, content=build_content, executable=False)

dev_env_tool = repository_rule(
    implementation = _dev_env_tool_impl,
    attrs = {
        "tool": attr.string(
            mandatory = True,
        ),
        "win_tool": attr.string(
            mandatory = True,
        ),
        "win_include": attr.string_list(
            mandatory = True,
        ),
        "win_path": attr.string(
            mandatory = False,
        ),
        "nix_label": attr.label(
            mandatory = True,
        ),
        "nix_include": attr.string_list(
            mandatory = True,
        ),
        "nix_path": attr.string(
            mandatory = True,
        ),
    },
)
