# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows")
load("//bazel/versions:dpm.version.bzl", "DPM_VERSION")

_BUNDLED_DPM = "cache/components/dpm/{}/dpm{}".format(DPM_VERSION, ".exe" if is_windows else "")

_UNPACK_AND_INITIALISE_DPM_HOME = """
set -euo pipefail
tar="$1" tarball="$2" dpm_home="$3" strip_components="$4" bundled_dpm="$5"
"$tar" -x -f "$tarball" -C "$dpm_home" --strip-components "$strip_components"
DPM_HOME="$dpm_home" HOME="$dpm_home" APPDATA="$dpm_home" "$dpm_home/$bundled_dpm" version > /dev/null
"""

def _unpacked_dpm_sdk_impl(ctx):
    dpm_home = ctx.actions.declare_directory(ctx.label.name)
    tar = ctx.toolchains["@tar.bzl//tar/toolchain:type"].tarinfo
    args = ctx.actions.args()
    args.add(tar.binary)
    args.add(ctx.file.src)
    args.add(dpm_home.path)
    args.add(str(ctx.attr.strip_components))
    args.add(_BUNDLED_DPM)
    ctx.actions.run_shell(
        command = _UNPACK_AND_INITIALISE_DPM_HOME,
        arguments = [args],
        inputs = [ctx.file.src],
        tools = [tar.binary],
        outputs = [dpm_home],
        env = tar.default_env,
        use_default_shell_env = True,
        mnemonic = "CompatUnpackDpmSdk",
        progress_message = "Unpacking %{label}",
        execution_requirements = {"no-cache": "1"},
    )
    return [DefaultInfo(
        files = depset([dpm_home]),
        runfiles = ctx.runfiles(files = [dpm_home]),
    )]

unpacked_dpm_sdk = rule(
    implementation = _unpacked_dpm_sdk_impl,
    attrs = {
        "src": attr.label(allow_single_file = True, mandatory = True),
        "strip_components": attr.int(default = 0),
    },
    toolchains = ["@tar.bzl//tar/toolchain:type"],
)
