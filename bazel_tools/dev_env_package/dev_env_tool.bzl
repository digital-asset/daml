# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/cpp:lib_cc_configure.bzl", "get_cpu_value")

def _create_build_content(rule_name, tools, win_paths, nix_paths):
    content = """
# DO NOT EDIT: automatically generated BUILD file for dev_env_package.bzl: {rule_name}
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all",
    srcs = glob(["**"]),
)
        """.format(rule_name = rule_name)

    for i in range (0, len(tools)):
        content += """
filegroup(
    name = "{tool}",
    srcs = select({{
        ":windows": ["{win_path}"],
        "//conditions:default": ["{nix_path}"],
    }}),
)
            """.format(
                tool = tools[i],
                win_path = win_paths[i],
                nix_path = nix_paths[i],
            )

    content += """
config_setting(
    name = "windows",
    values = {"cpu": "x64_windows"},
    visibility = ["//visibility:private"],
)
"""

    return content

def _dev_env_tool_impl(ctx):
    if get_cpu_value(ctx) == "x64_windows":
        ps = ctx.which("powershell")
        ps_result = ctx.execute([ps, "-Command", "dadew enable; dadew where"], quiet = True)
        if ps_result.return_code != 0:
            fail("Failed to obtain dadew location.\n Exit code %d.\n%s\n%s" %
                 (ps_result.return_code, ps_result.stdout, ps_result.stderr))

        dadew = ps_result.stdout.splitlines(keepends = False)[0]
        tool_home = "%s\\scoop\\apps\\%s\\current" % (dadew, ctx.attr.win_tool)
        for i in ctx.attr.win_include:
            ctx.symlink("%s\\%s" % (tool_home, i), "%s\\%s" % (ctx.path(""), ctx.attr.win_include_as.get(i, i)))

    else:
        tool_home = "../%s" % ctx.attr.nix_label.name
        for i in ctx.attr.nix_include:
            ctx.symlink("%s/%s" % (tool_home, i), "%s/%s" % (ctx.path(""), i))

    build_path = ctx.path("BUILD")
    build_content = _create_build_content(
        rule_name = ctx.name,
        tools = ctx.attr.tools,
        win_paths = ctx.attr.win_paths,
        nix_paths = ctx.attr.nix_paths,
    )
    ctx.file(build_path, content = build_content, executable = False)

dev_env_tool = repository_rule(
    implementation = _dev_env_tool_impl,
    attrs = {
        "tools": attr.string_list(
            mandatory = True,
        ),
        "win_tool": attr.string(
            mandatory = True,
        ),
        "win_include": attr.string_list(
            mandatory = True,
        ),
        "win_include_as": attr.string_dict(
            mandatory = False,
            default = {},
        ),
        "win_paths": attr.string_list(
            mandatory = False,
        ),
        "nix_label": attr.label(
            mandatory = False,
        ),
        "nix_include": attr.string_list(
            mandatory = True,
        ),
        "nix_paths": attr.string_list(
            mandatory = True,
        ),
    },
)
