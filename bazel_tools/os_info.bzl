# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/cpp:lib_cc_configure.bzl", "get_cpu_value")

_os_info_bzl_template = """
cpu_value = "{CPU_VALUE}"
is_darwin = cpu_value == "darwin"
is_linux = cpu_value == "k8"
is_windows = cpu_value == "x64_windows"
os_name = "macos" if is_darwin else "linux" if is_linux else "windows"
"""

def _os_info_impl(repository_ctx):
    cpu = get_cpu_value(repository_ctx)
    os_info_substitutions = {
        "CPU_VALUE": cpu,
    }
    repository_ctx.file(
        "os_info.bzl",
        _os_info_bzl_template.format(**os_info_substitutions),
        False,
    )
    repository_ctx.file(
        "BUILD",
        "",
        False,
    )

os_info = repository_rule(
    implementation = _os_info_impl,
    local = True,
)
