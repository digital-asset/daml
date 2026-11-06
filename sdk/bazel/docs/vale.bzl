# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "cpu_value", "is_windows")

_VALE_PLATFORM = {
    "k8": "Linux_64-bit",
    "aarch64": "Linux_arm64",
    "darwin_x86_64": "macOS_64-bit",
    "darwin_arm64": "macOS_arm64",
    "x64_windows": "Windows_64-bit",
}

def _vale_impl(ctx):
    ctx.download_and_extract(
        url = "https://github.com/errata-ai/vale/releases/download/v{version}/vale_{version}_{platform}.{extension}".format(
            version = ctx.attr.version,
            platform = _VALE_PLATFORM[cpu_value],
            extension = "zip" if is_windows else "tar.gz",
        ),
        sha256 = ctx.attr.sha256[cpu_value],
    )
    ctx.file(
        "BUILD.bazel",
        content = 'exports_files(["{binary}"], visibility = ["//visibility:public"])\n'.format(
            binary = "vale.exe" if is_windows else "vale",
        ),
    )

vale = repository_rule(
    implementation = _vale_impl,
    attrs = {
        "sha256": attr.string_dict(mandatory = True),
        "version": attr.string(mandatory = True),
    },
)
