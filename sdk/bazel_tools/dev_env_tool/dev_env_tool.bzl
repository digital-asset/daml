# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@bazel_tools//tools/cpp:lib_cc_configure.bzl", "get_cpu_value")
load("@rules_sh//sh:posix.bzl", "posix")

def _create_build_content(rule_name, is_windows, tools, required_tools, win_paths, nix_paths):
    content = """
# DO NOT EDIT: automatically generated BUILD file for dev_env_tool.bzl: {rule_name}

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all",
    srcs = glob(["**"]),
)
""".format(rule_name = rule_name)

    for i in range(0, len(tools)):
        if is_windows:
            content += """
# Running tools with `bazel run` is not supported on Windows.
filegroup(
    name = "{tool}",
    srcs = ["{path}"],
)
""".format(
                tool = tools[i],
                path = win_paths[i],
            )
        else:
            content += """
sh_binary(
    name = "{tool}",
    srcs = ["{path}"],
    data = {dependencies},
)
""".format(
                tool = tools[i],
                dependencies = [":{}".format(dep) for dep in required_tools.get(tools[i], [])],
                path = nix_paths[i],
            )

    content += """
config_setting(
    name = "windows",
    values = {"cpu": "x64_windows"},
    visibility = ["//visibility:private"],
)
"""

    return content

def dadew_where(ctx, ps):
    ps = ctx.which("powershell")
    ps_result = ctx.execute([ps, "-Command", "dadew where"], quiet = True)

    if ps_result.return_code != 0:
        fail("Failed to obtain dadew location.\nExit code %d.\n%s\n%s" %
             (ps_result.return_code, ps_result.stdout, ps_result.stderr))

    return ps_result.stdout.splitlines()[0]

def dadew_tool_home(dadew, tool):
    return "%s\\scoop\\apps\\%s\\current" % (dadew, tool)

def _find_files_recursive(ctx, find, root):
    find_result = ctx.execute([find, "-L", root, "-type", "f", "-print0"])

    if find_result.return_code != 0:
        fail("Failed to list files contained in '%s':\nExit code %d\n%s\n%s." %
             (root, find_result.return_code, find_result.stdout, find_result.stderr))

    return [
        f[len(root) + 1:]
        for f in find_result.stdout.split("\0")
        if f and f.startswith(root)
    ]

def _symlink_files_recursive(ctx, find, source, dest):
    files = _find_files_recursive(ctx, find, source)
    for f in files:
        ctx.symlink("%s/%s" % (source, f), "%s/%s" % (dest, f))

def _dadew_impl(ctx):
    ctx.file("BUILD.bazel", executable = False)
    if get_cpu_value(ctx) == "x64_windows":
        ps = ctx.which("powershell")
        dadew = dadew_where(ctx, ps)
        ctx.file("dadew.bzl", executable = False, content = """
dadew = r"{dadew}"
def dadew_tool_home(tool):
    return r"%s\\scoop\\apps\\%s\\current" % (dadew, tool)
""".format(dadew = dadew))
    else:
        ctx.file("dadew.bzl", executable = False, content = """
dadew = None
def dadew_tool_home(tool):
    return None
""")

dadew = repository_rule(
    implementation = _dadew_impl,
    configure = True,
    local = True,
)

def _dev_env_tool_impl(ctx):
    is_windows = get_cpu_value(ctx) == "x64_windows"
    if is_windows:
        ps = ctx.which("powershell")
        dadew = dadew_where(ctx, ps)
        find = dadew_tool_home(dadew, "msys2") + "\\usr\\bin\\find.exe"
        tool_home = dadew_tool_home(dadew, ctx.attr.win_tool)
        for i in ctx.attr.win_include:
            src = "%s\\%s" % (tool_home, i)
            dst = ctx.attr.win_include_as.get(i, i)
            if ctx.attr.prefix:
                dst = "%s\\%s" % (ctx.attr.prefix, dst)
            _symlink_files_recursive(ctx, find, src, dst)
    else:
        find = "find"
        tool_home = "../%s" % ctx.attr.nix_label.name
        for i in ctx.attr.nix_include:
            src = "%s/%s" % (tool_home, i)
            dst = i
            if ctx.attr.prefix:
                dst = "%s/%s" % (ctx.attr.prefix, dst)
            _symlink_files_recursive(ctx, find, src, dst)

    build_path = ctx.path("BUILD")
    build_content = _create_build_content(
        rule_name = ctx.name,
        is_windows = is_windows,
        tools = ctx.attr.tools,
        required_tools = ctx.attr.required_tools,
        win_paths = [
            "%s/%s" % (ctx.attr.prefix, path)
            for path in ctx.attr.win_paths
        ] if ctx.attr.prefix else ctx.attr.win_paths,
        nix_paths = [
            "%s/%s" % (ctx.attr.prefix, path)
            for path in ctx.attr.nix_paths
        ] if ctx.attr.prefix else ctx.attr.nix_paths,
    )
    ctx.file(build_path, content = build_content, executable = False)

dev_env_tool = repository_rule(
    implementation = _dev_env_tool_impl,
    attrs = {
        "tools": attr.string_list(
            mandatory = True,
        ),
        "required_tools": attr.string_list_dict(
            mandatory = False,
            default = {},
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
        "prefix": attr.string(),
    },
    configure = True,
    local = True,
)

def _dadew_sh_posix_config_impl(repository_ctx):
    ps = repository_ctx.which("powershell")
    dadew = dadew_where(repository_ctx, ps)
    msys2_usr_bin = dadew_tool_home(dadew, "msys2") + "\\usr\\bin"
    commands = {}
    for cmd in posix.commands:
        for ext in [".exe", ""]:
            path = "%s\\%s%s" % (msys2_usr_bin, cmd, ext)
            if repository_ctx.path(path).exists:
                commands[cmd] = path
    repository_ctx.file("BUILD.bazel", executable = False, content = """
load("@rules_sh//sh:posix.bzl", "sh_posix_toolchain")
sh_posix_toolchain(
    name = "dadew_posix",
    cmds = {{
        {commands},
    }},
)
toolchain(
    name = "dadew_posix_toolchain",
    toolchain = "dadew_posix",
    toolchain_type = "@rules_sh//sh/posix:toolchain_type",
    exec_compatible_with = [
        "@bazel_tools//platforms:x86_64",
        "@bazel_tools//platforms:windows",
    ],
    target_compatible_with = [
        "@bazel_tools//platforms:x86_64",
        "@bazel_tools//platforms:windows",
    ],
)
""".format(
        commands = ",\n        ".join([
            'r"{cmd}": r"{path}"'.format(cmd = cmd, path = cmd_path).replace("\\", "/")
            for (cmd, cmd_path) in commands.items()
            if cmd_path
        ]),
    ))

_dadew_sh_posix_config = repository_rule(
    implementation = _dadew_sh_posix_config_impl,
    configure = True,
    local = True,
)

def dadew_sh_posix_configure(name = "dadew_sh_posix"):
    _dadew_sh_posix_config(name = name)
    native.register_toolchains("@%s//:dadew_posix_toolchain" % name)

def _create_shim(repository_ctx, shim_exe, *, shim, path, extension):
    """Create a shim file for Scoop's shim.exe utility.

    See [ScoopInstaller/Shim][shim-gh] for further information.

    [shim-gh]: https://github.com/ScoopInstaller/Shim#readme
    """
    if extension in [".exe", ".cmd", ".bat"]:
        repository_ctx.file(shim + ".shim", executable = False, content = "path = " + str(path))
        result = shim + ".exe"

        # Will create a copy on Windows
        repository_ctx.symlink(shim_exe, result)
    else:
        # We assume that files without standard extension are scripts (bash,
        # perl, ...), e.g. automake. Scoop's shim doesn't work on these. So, we
        # assume that, since they are shell scripts in the first place, they
        # will only be called in a shell context and simply create a copy.
        repository_ctx.symlink(path, shim)
        result = shim + extension
    return result

def _dadew_binary_bundle_impl(repository_ctx):
    ps = repository_ctx.which("powershell")
    dadew = dadew_where(repository_ctx, ps)

    scoop_home = dadew_tool_home(dadew, "scoop")
    shim_exe = repository_ctx.path(paths.join(scoop_home, "supporting/shimexe/bin/shim.exe"))

    tool_home = dadew_tool_home(dadew, repository_ctx.attr.tool)
    extensions = ["", ".exe", ".cmd", ".bat"]
    missing = []
    found = []
    for path_str in repository_ctx.attr.paths:
        parts = path_str.split(":", 1)
        if len(parts) == 1:
            [path_str] = parts
            shim = path_str
        elif len(parts) == 2:
            [path_str, shim] = parts
        else:
            fail("Invalid path '{}': Expected 'PATH' or 'PATH:SHIM'.".format(path_str))
        for ext in extensions:
            path = repository_ctx.path(paths.join(tool_home, path_str + ext))
            if path.exists:
                break
            else:
                path = None
        if path:
            found.append(
                _create_shim(repository_ctx, shim_exe, shim = shim, path = path, extension = ext),
            )
        else:
            missing.append(path_str)
    repository_ctx.file("BUILD.bazel", executable = False, content = """\
load("@rules_sh//sh:sh.bzl", "sh_binaries")

package(default_visibility = ["//visibility:public"])

sh_binaries(
    name = "tools",
    srcs = {tools},
)
""".format(
        tools = repr(found),
    ))
    if missing:
        fail("Missing paths in '{}': {}".format(tool_home, ", ".join(missing)))

dadew_binary_bundle = repository_rule(
    _dadew_binary_bundle_impl,
    attrs = {
        "tool": attr.string(
            mandatory = True,
            doc = "Import binaries from this Scoop tool, i.e. an entry under the `dev-env/windows/manifests` folder.",
        ),
        "paths": attr.string_list(
            mandatory = True,
            doc = """\
Import these binaries from the Scoop tool into Bazel.

Items can either be paths relative to the Scoop tool installation path, or colon (`:`) separated tuples where the first item is the path relative to the Scoop tool installation and the second item the name that it should be mapped to within Bazel.
""",
        ),
    },
    configure = True,
    local = True,
    doc = """\
Generate a `sh_binaries` containing a set of binaries provided by a Scoop tool.

Use this to import binaries provided by Scoop into the Bazel build in a
hermetic way. This generates [Scoop shims][scoop-shim] for the specified
binaries within Bazel's execution root and constructs a `sh_binaries` that
contains these shims. The generated PATH make variable will only cover tools
explicitly listed on this rule.

[scoop-shim]: https://github.com/ScoopInstaller/Shim#readme
""",
)
