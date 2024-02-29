# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/cpp:lib_cc_configure.bzl", "get_cpu_value")

_os_info_bzl_template = """
cpu_value = "{CPU_VALUE}"
is_darwin = cpu_value == "darwin" or cpu_value == "darwin_arm64"
is_darwin_arm64 = cpu_value == "darwin_arm64"
is_linux_intel = cpu_value == "k8"
is_linux_arm = cpu_value == "aarch64"
is_linux = is_linux_intel or is_linux_arm
is_windows = cpu_value == "x64_windows"
os_name = "macos" if is_darwin else "linux" if is_linux_intel or is_linux_arm else "windows"
is_intel = is_windows or is_darwin or is_linux_intel
is_arm = is_darwin_arm64 or is_linux_arm
"""

def _os_info_impl(repository_ctx):
    cpu = get_cpu_value(repository_ctx)
    known_cpu_values = [
        "aarch64",  # linux arm64
        "darwin",  # macOS amd64
        "darwin_arm64",  # macOS arm64
        "k8",  # linux amd64
        "x64_windows",
    ]
    if cpu not in known_cpu_values:
        fail("Unknown OS type {}, expected one of {}".format(cpu, ", ".join(known_cpu_values)))
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
