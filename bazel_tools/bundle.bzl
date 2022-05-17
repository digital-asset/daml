# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_cc//cc:action_names.bzl", "ACTION_NAME_GROUPS")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain")

BinaryBundleInfo = provider(fields = ["tool_dirs"])
LibraryBundleInfo = provider(fields = ["library_dirs"])

def _to_var_name(label_name):
    return label_name.upper().replace("-", "_")

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
    binary_bundle_info = BinaryBundleInfo(tool_dirs = tool_dirs)

    path_separator = ":"  # TODO[AH] Handle Windows
    path = path_separator.join(tool_dirs.to_list())
    template_variable_info = platform_common.TemplateVariableInfo({
        "{}_PATH".format(_to_var_name(ctx.label.name)): path,
    })

    return [
        default_info,
        binary_bundle_info,
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

The make variable is called `<RULE_NAME>_PATH`.
""",
)

def _binary_bundle_impl(ctx):
    runfiles = ctx.runfiles(files = ctx.files.tools + ctx.files.data)
    for tool_dep in ctx.attr.tools:
        runfiles = runfiles.merge(tool_dep[DefaultInfo].default_runfiles)
    for bundle_dep in ctx.attr.deps:
        runfiles = runfiles.merge(bundle_dep[DefaultInfo].default_runfiles)
    for data_dep in ctx.attr.data:
        runfiles = runfiles.merge(data_dep[DefaultInfo].default_runfiles)

    default_info = DefaultInfo(
        files = depset(direct = ctx.files.tools + ctx.files.deps),
        runfiles = runfiles,
    )

    tool_dirs = depset(transitive = [
        depset(direct = [tool.dirname])
        for tool in ctx.files.tools
    ] + [
        bundle_dep[BinaryBundleInfo].tool_dirs
        for bundle_dep in ctx.attr.deps
    ])
    binary_bundle_info = BinaryBundleInfo(tool_dirs = tool_dirs)

    path_separator = ":"  # TODO[AH] Handle Windows
    path = path_separator.join(tool_dirs.to_list())
    template_variable_info = platform_common.TemplateVariableInfo({
        "{}_PATH".format(_to_var_name(ctx.label.name)): path,
    })

    return [
        default_info,
        binary_bundle_info,
        template_variable_info,
    ]

binary_bundle = rule(
    _binary_bundle_impl,
    attrs = {
        "tools": attr.label_list(allow_files = True),
        "deps": attr.label_list(),
        "data": attr.label_list(allow_files = True),
    },
    doc = """\
Bundles multiple binaries and expose a PATH value in a make variable.

The make variable is called `<RULE_NAME>_PATH`.
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

    path_separator = ":"  # TODO[AH] Handle Windows
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
        "libs": attr.label_list(allow_files = True),
        "deps": attr.label_list(),
        "data": attr.label_list(allow_files = True),
    },
    doc = """\
Bundles multiple libraries and expose a LIBRARY_PATH value in a make variable.

The make variable is called `<RULE_NAME>_LIBRARY_PATH`.
""",
)
