# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "cpu_value", "is_windows")

runfiles_library = """
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---
"""

def _dpm_binary_impl(ctx):
    oras_dir = ctx.path("oras_binary")

    oras_plat_mapping = {
        "aarch64": "linux_arm64.tar.gz",
        "darwin_x86_64": "darwin_amd64.tar.gz",
        "darwin_arm64": "darwin_arm64.tar.gz",
        "k8": "linux_amd64.tar.gz",
        "x64_windows": "windows_amd64.zip",
    }

    # We shouldn't ever need to update oras, so keep its hashes here
    oras_version = "1.3.0"
    oras_shas_mapping = {
        "linux_arm64": "7649738b48fde10542bcc8b0e9b460ba83936c75fb5be01ee6d4443764a14352",
        "darwin_amd64": "82c33f7da8430ea7fa7e7bdf7721be0a0d0481e5ccb2472ea438490d5e8641a9",
        "darwin_arm64": "e10c6552c02d5a7c7eaf7170d3b6f7f094b675a98a1e0edf4d4478a909447245",
        "linux_amd64": "6cdc692f929100feb08aa8de584d02f7bcc30ec7d88bc2adc2054d782db57c64",
        "windows_amd64": "b050e93aa0dc7a79a61fa8e4074dfa302c41d4af01b634fe393c5dd687536aee",
    }

    ctx.download_and_extract(
        output = oras_dir,
        url = "https://github.com/oras-project/oras/releases/download/v{}/oras_{}_{}".format(oras_version, oras_version, oras_plat_mapping[cpu_value]),
        sha256 = oras_shas_mapping[oras_plat_mapping[cpu_value]],
    )

    ctx.file(
        "oras.sh",
        content =
            """#!/usr/bin/env bash
{runfiles_library}
$(rlocation oras_binary/oras.exe) $@
""".format(runfiles_library = runfiles_library),
    ) if is_windows else None

    ctx.file(
        "BUILD",
        content =
            """
load("@os_info//:os_info.bzl", "is_windows")
package(default_visibility = ["//visibility:public"])
sh_binary(
  name = "oras",
  srcs = [":oras_binary/oras"],
) if not is_windows else sh_binary(
  name = "oras",
  srcs = [":oras.sh"],
  deps = ["@bazel_tools//tools/bash/runfiles"],
  data = ["oras_binary/oras.exe"],
)

genrule(
    name = "dpm_binary",
    srcs = [],
    outs = ["dpm"],
    cmd = \"""
    set -eoux pipefail
    ORAS="$(location :oras)"
    export HOME=$$(dirname $@)
    $$ORAS pull --platform "{oras_platform}" -o $$(dirname $@) europe-docker.pkg.dev/da-images/public/components/dpm:{version}
    chmod +x $@
    downloaded_sha=$$(sha256sum $@)
    if [ "$${{downloaded_sha%% *}}" != "{dpm_sha}" ]; then
      echo "Invalid SHA256 for DPM, expected {dpm_sha}, got $${{downloaded_sha%% *}}"
      exit 1
    fi
  \""",
    tools = [
        ":oras",
    ],
    visibility = ["//visibility:public"],
)

""".format(
                version = ctx.attr.version,
                oras_platform = oras_plat_mapping[cpu_value].replace("_", "/"),
                dpm_sha = ctx.attr.sha256[oras_plat_mapping[cpu_value]],
            ),
    )
    return None

dpm_binary = repository_rule(
    implementation = _dpm_binary_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        # Hashes taken from the files themselves, not the OCI artifacts
        # See `layers[0].digest` in the manifest for each platform
        "sha256": attr.string_dict(mandatory = True),
    },
)
