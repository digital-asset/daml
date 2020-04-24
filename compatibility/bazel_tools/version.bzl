# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:paths.bzl", "paths")
load(":daml_sdk.bzl", "runfiles_library")

def _string_keyed_label_dict_to_label_keyed_string_dict(string_keyed, sep = ";"):
    label_keyed = {}
    for (string, label) in string_keyed.items():
        label_keyed.setdefault(label, []).append(string)
    return {
        label: sep.join(strings)
        for (label, strings) in label_keyed.items()
    }

def _label_keyed_string_dict_to_string_keyed_label_dict(label_keyed, sep = ";"):
    string_keyed = {}
    for (label, strings) in label_keyed.items():
        string_keyed.update([
            (string, label)
            for string in strings.split(sep)
        ])
    return string_keyed

VersionInfo = provider(fields = ["version"])

def _version_flag_impl(ctx):
    return [VersionInfo(version = ctx.build_setting_value)]

version_flag = rule(
    implementation = _version_flag_impl,
    build_setting = config.string(flag = True),
)

def _get_version(version, known_versions):
    if not version in known_versions:
        fail("Version {version} not available".format(version = version), "versions")
    return known_versions[version]

def _versioned_file_impl(ctx, executable = False):
    target = _get_version(
        ctx.attr.flag[VersionInfo].version,
        _label_keyed_string_dict_to_string_keyed_label_dict(ctx.attr.versions),
    )
    return [target[DefaultInfo]]

def _wrap_binary(ctx, target, wrapper, before = "", after = ""):
    binary = target[DefaultInfo].files_to_run.executable
    binary_runfiles = target[DefaultInfo].default_runfiles
    binary_runpath = paths.join(ctx.workspace_name, paths.relativize(binary.path, binary.root.path))
    ctx.actions.write(
        output = wrapper,
        content = """\
#!/usr/bin/env bash
{runfiles_library}
{before}
$(rlocation {runpath}) "$@"
{after}
""".format(
            runfiles_library = runfiles_library,
            before = before,
            runpath = binary_runpath,
            after = after,
        ),
        is_executable = True,
    )
    return DefaultInfo(
        executable = wrapper,
        files = depset(direct = [wrapper]),
        runfiles = binary_runfiles.merge(
            ctx.runfiles(
                files = [wrapper],
                transitive_files = ctx.attr._bash_runfiles.files,
                # collect_data is deprecated, see
                # https://docs.bazel.build/versions/master/skylark/rules.html#runfiles-features-to-avoid
                collect_data = True,
            ),
        ),
    )

def _versioned_binary_impl(ctx):
    target = _get_version(
        ctx.attr.flag[VersionInfo].version,
        _label_keyed_string_dict_to_string_keyed_label_dict(ctx.attr.versions),
    )
    wrapper = ctx.actions.declare_file("%s.sh" % ctx.label.name)
    default_info = _wrap_binary(ctx, target, wrapper, before = "set -euo pipefail")
    return [default_info]

_versioned_file = rule(
    _versioned_file_impl,
    attrs = {
        "flag": attr.label(providers = [VersionInfo]),
        "versions": attr.label_keyed_string_dict(),
    },
)

_versioned_binary = rule(
    _versioned_binary_impl,
    attrs = {
        "flag": attr.label(providers = [VersionInfo]),
        "versions": attr.label_keyed_string_dict(),
        "_bash_runfiles": attr.label(default = "@bazel_tools//tools/bash/runfiles"),
    },
    executable = True,
)

def versioned_file(name, flag, versions, **kwargs):
    _versioned_file(
        name = name,
        flag = flag,
        versions = _string_keyed_label_dict_to_label_keyed_string_dict(versions),
        **kwargs
    )

def versioned_binary(name, flag, versions, **kwargs):
    _versioned_binary(
        name = name,
        flag = flag,
        versions = _string_keyed_label_dict_to_label_keyed_string_dict(versions),
        **kwargs
    )

def _version_transition(settings, attr):
    new_settings = {
        "//:sdk_version": settings["//:sdk_version"],
        "//:platform_version": settings["//:platform_version"],
    }

    sdk_versions = [
        tag[len("sdk_version="):]
        for tag in attr.tags
        if tag.startswith("sdk_version=")
    ]
    if len(sdk_versions) > 1:
        fail("Found more than one sdk_version tag", "tags")
    if sdk_versions:
        new_settings["//:sdk_version"] = sdk_versions[0]

    platform_versions = [
        tag[len("platform_version="):]
        for tag in attr.tags
        if tag.startswith("platform_version=")
    ]
    if len(platform_versions) > 1:
        fail("Found more than one platform_version tag", "tags")
    if platform_versions:
        new_settings["//:platform_version"] = platform_versions[0]

    return [new_settings]

version_transition = transition(
    implementation = _version_transition,
    inputs = ["//:sdk_version", "//:platform_version"],
    outputs = ["//:sdk_version", "//:platform_version"],
)

def _matrix_test_impl(ctx):
    wrapper = ctx.actions.declare_file("%s.sh" % ctx.label.name)
    default_info = _wrap_binary(ctx, ctx.attr.test, wrapper, before = "set -euo pipefail")
    return [default_info]

matrix_test = rule(
    _matrix_test_impl,
    cfg = version_transition,
    attrs = {
        "test": attr.label(),
        "_bash_runfiles": attr.label(default = "@bazel_tools//tools/bash/runfiles"),
        "_whitelist_function_transition": attr.label(
             default = "@bazel_tools//tools/whitelists/function_transition_whitelist"
         ),
    },
    test = True
)
