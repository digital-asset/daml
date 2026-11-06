# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def _java_runtime_version_transition_impl(_settings, attr):
    return {"//command_line_option:java_runtime_version": attr.java_runtime_version}

_java_runtime_version_transition = transition(
    implementation = _java_runtime_version_transition_impl,
    inputs = [],
    outputs = ["//command_line_option:java_runtime_version"],
)

def _java_runtime_binary_impl(ctx):
    binary = ctx.attr.binary[0][DefaultInfo]
    executable = binary.files_to_run.executable
    return [DefaultInfo(
        files = depset([executable]),
        runfiles = ctx.runfiles([executable]).merge(binary.default_runfiles),
    )]

java_runtime_binary = rule(
    implementation = _java_runtime_binary_impl,
    attrs = {
        "binary": attr.label(
            cfg = _java_runtime_version_transition,
            executable = True,
            mandatory = True,
        ),
        "java_runtime_version": attr.string(mandatory = True),
    },
)
