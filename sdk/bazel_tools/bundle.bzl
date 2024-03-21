# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_cc//cc:action_names.bzl", "ACTION_NAME_GROUPS")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain")
load("@rules_sh//sh:sh.bzl", "ShBinariesInfo")

BinaryBundleInfo = provider(fields = ["tool_dirs"])
LibraryBundleInfo = provider(fields = ["library_dirs"])

def _to_var_name(label_name):
    """Turn a label name into a template variable info.

    Uses all upper case variable names with `_` as separator.
    """
    return label_name.upper().replace("-", "_")

def _path_separator(ctx):
    """Generate the path list separator."""

    # Use ':' even on Windows because msys2 will automatically convert such
    # path lists to the Windows format, using ';' as separator and 'C:\'
    # syntax. Be sure that absolute paths use the format '/c/...' instead of
    # 'C:\...'. See https://www.msys2.org/docs/filesystem-paths/.
    return ":"

def _cc_toolchain_binary_bundle_impl(ctx):
    cc_toolchain_info = find_cc_toolchain(ctx)

    files = cc_toolchain_info.all_files
    runfiles = ctx.runfiles(transitive_files = files)

    default_info = DefaultInfo(
        files = files,
        runfiles = runfiles,
    )

    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain_info,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    action_names = ACTION_NAME_GROUPS.all_cc_compile_actions + ACTION_NAME_GROUPS.all_cc_link_actions + ACTION_NAME_GROUPS.all_cpp_compile_actions
    tools = []
    for action_name in action_names:
        is_enabled = cc_common.action_is_enabled(
            feature_configuration = feature_configuration,
            action_name = action_name,
        )
        if not is_enabled:
            continue
        tool = cc_common.get_tool_for_action(
            feature_configuration = feature_configuration,
            action_name = action_name,
        )
        if tool:
            tools.append(tool)

    tool_dirs = depset(transitive = [
        depset(direct = [paths.dirname(tool)])
        for tool in tools
    ])
    sh_binary_info = ShBinariesInfo(
        executables = {},
        paths = tool_dirs,
    )

    path_separator = _path_separator(ctx)
    path = path_separator.join(tool_dirs.to_list())
    template_variable_info = platform_common.TemplateVariableInfo({
        "{}_PATH".format(_to_var_name(ctx.label.name)): path,
    })

    return [
        default_info,
        sh_binary_info,
        template_variable_info,
    ]

cc_toolchain_binary_bundle = rule(
    _cc_toolchain_binary_bundle_impl,
    attrs = {
        "_cc_toolchain": attr.label(default = Label("@rules_cc//cc:current_cc_toolchain")),
    },
    fragments = ["cpp"],
    toolchains = ["@rules_cc//cc:toolchain_type"],
    doc = """\
Bundle the CC toolchain tools and expose a PATH value in a make variable.

This bundle can be used as a dependency to `sh_binaries` rules or as a
`toolchain` dependency to `genrule`s. In the latter case the make variable
exposes a `PATH` variable to the bundled tools.

The make variable is called `<RULE_NAME>_PATH`.

Refer to the [Bazel documentation][bazel-doc] for more information on Bazel
make variables.

[bazel-doc]: https://bazel.build/reference/be/make-variables
""",
)

def _append_if_not_none(lst, item):
    if item:
        lst.append(item)

def _library_bundle_impl(ctx):
    libraries = []
    files = []
    cc_infos = []
    for lib in ctx.attr.libs:
        cc_info = lib[CcInfo]
        cc_infos.append(cc_info)
        for linker_input in cc_info.linking_context.linker_inputs.to_list():
            for library in linker_input.libraries:
                _append_if_not_none(libraries, library.dynamic_library)
                _append_if_not_none(files, library.dynamic_library)
                _append_if_not_none(files, library.resolved_symlink_dynamic_library)
                _append_if_not_none(libraries, library.interface_library)
                _append_if_not_none(files, library.interface_library)
                _append_if_not_none(files, library.resolved_symlink_interface_library)
                _append_if_not_none(libraries, library.interface_library)
                _append_if_not_none(files, library.interface_library)

    cc_info = cc_common.merge_cc_infos(direct_cc_infos = cc_infos)

    runfiles = ctx.runfiles(files = files + ctx.files.data)
    for lib_dep in ctx.attr.libs:
        runfiles = runfiles.merge(lib_dep[DefaultInfo].default_runfiles)
    for bundle_dep in ctx.attr.deps:
        runfiles = runfiles.merge(bundle_dep[DefaultInfo].default_runfiles)
    for data_dep in ctx.attr.data:
        runfiles = runfiles.merge(data_dep[DefaultInfo].default_runfiles)

    default_info = DefaultInfo(
        files = depset(direct = files + ctx.files.deps),
        runfiles = runfiles,
    )

    library_dirs = depset(transitive = [
        depset(direct = [lib.dirname])
        for lib in libraries
    ] + [
        bundle_dep[LibraryBundleInfo].library_dirs
        for bundle_dep in ctx.attr.deps
    ])
    library_bundle_info = LibraryBundleInfo(library_dirs = library_dirs)

    path_separator = _path_separator(ctx)
    library_path = path_separator.join(library_dirs.to_list())
    template_variable_info = platform_common.TemplateVariableInfo({
        "{}_LIBRARY_PATH".format(_to_var_name(ctx.label.name)): library_path,
    })

    return [
        cc_info,
        default_info,
        library_bundle_info,
        template_variable_info,
    ]

library_bundle = rule(
    _library_bundle_impl,
    attrs = {
        "libs": attr.label_list(
            allow_files = True,
            doc = "Include these cc_library targets (or equivalent) in the bundle.",
        ),
        "deps": attr.label_list(
            doc = "Include these transitive library bundle dependencies.",
        ),
        "data": attr.label_list(
            allow_files = True,
            doc = "Include additional data files when running build actions that depend on this bundle.",
        ),
    },
    doc = """\
Bundles multiple libraries and exposes a LIBRARY_PATH value in a make variable.

This bundle can be used as a dependency to other `library_bundle` rules or as a
`toolchain` dependency to `genrule`s. In the latter case the make variable
exposes a `LIBRARY_PATH` variable to the bundled static and dynamic libraries.

The make variable is called `<RULE_NAME>_LIBRARY_PATH`.

Refer to the [Bazel documentation][bazel-doc] for more information on Bazel
make variables.

[bazel-doc]: https://bazel.build/reference/be/make-variables
""",
)
