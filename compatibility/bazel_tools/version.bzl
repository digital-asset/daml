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

def _versioned_file_impl(ctx, executable = False):
    version = ctx.attr.flag[VersionInfo].version
    registry = _label_keyed_string_dict_to_string_keyed_label_dict(ctx.attr.versions)
    if not version in registry:
        fail("Version {version} not available in registry".format(version = version), "versions")
    target = registry[version]
    return [target[DefaultInfo]]

def _versioned_binary_impl(ctx):
    version = ctx.attr.flag[VersionInfo].version
    registry = _label_keyed_string_dict_to_string_keyed_label_dict(ctx.attr.versions)
    if not version in registry:
        fail("Version {version} not available in registry".format(version = version), "versions")
    target = registry[version]
    executable = target[DefaultInfo].files_to_run.executable
    executable_runpath = paths.join(ctx.workspace_name, paths.relativize(executable.path, executable.root.path))
    output = ctx.outputs.output
    ctx.actions.write(
        output = output,
        content = """\
#!/usr/bin/env bash
{runfiles_library}
$(rlocation {executable}) "$@"
""".format(
            runfiles_library = runfiles_library,
            executable = executable_runpath,
        ),
        is_executable = True,
    )
    return [DefaultInfo(
        executable = output,
        files = depset(direct = [output]),
        runfiles = target[DefaultInfo].default_runfiles.merge(
            ctx.runfiles(
                files = [output],
                transitive_files = ctx.attr._runfiles.files,
                collect_data = True,
            ),
        ),
    )]

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
        "output": attr.output(),
        "_runfiles": attr.label(default = "@bazel_tools//tools/bash/runfiles"),
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
        output = "%s.sh" % name,
        **kwargs
    )
