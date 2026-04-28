# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "cpu_value", "is_windows")

_GRPCURL_PLATFORM = {
    "k8": "linux_x86_64",
    "aarch64": "linux_arm64",
    "darwin_x86_64": "osx_x86_64",
    "darwin_arm64": "osx_arm64",
    "x64_windows": "windows_x86_64",
}

def _grpcurl_impl(ctx):
    plat = _GRPCURL_PLATFORM[cpu_value]
    ext = "zip" if is_windows else "tar.gz"
    ctx.download_and_extract(
        url = "https://github.com/fullstorydev/grpcurl/releases/download/v{version}/grpcurl_{version}_{plat}.{ext}".format(
            version = ctx.attr.version,
            plat = plat,
            ext = ext,
        ),
        sha256 = ctx.attr.sha256[cpu_value],
    )
    bin_name = "grpcurl.exe" if is_windows else "grpcurl"
    ctx.file(
        "BUILD",
        content = """
package(default_visibility = ["//visibility:public"])
filegroup(
    name = "bin",
    srcs = ["{bin}"],
)
""".format(bin = bin_name),
    )

grpcurl = repository_rule(
    implementation = _grpcurl_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sha256": attr.string_dict(mandatory = True),
    },
)
