# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows", "os_name")
load("@io_bazel_rules_scala//scala:scala_cross_version.bzl", "default_maven_server_urls")

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

def _damlc_legacy_impl(ctx):
    out_dir = ctx.path("damlc")

    ctx.download_and_extract(
        output = out_dir,
        url = "https://github.com/digital-asset/daml/releases/download/v{}/daml-sdk-{}-{}.tar.gz".format(ctx.attr.version, ctx.attr.version, ctx.attr.os_name),
        sha256 = ctx.attr.sha256[ctx.attr.os_name],
        stripPrefix = "sdk-{}/damlc".format(ctx.attr.version),
    )

    ctx.file(
        "damlc.sh",
        content =
            """#!/usr/bin/env bash
{runfiles_library}
$(rlocation damlc_legacy/damlc/damlc.exe) $@
""".format(runfiles_library = runfiles_library),
    ) if is_windows else None

    ctx.file(
        "BUILD",
        content =
            """
load("@os_info//:os_info.bzl", "is_windows")
package(default_visibility = ["//visibility:public"])
sh_binary(
  name = "{name}",
  srcs = [":damlc/damlc"],
) if not is_windows else sh_binary(
  name = "{name}",
  srcs = [":damlc.sh"],
  deps = ["@bazel_tools//tools/bash/runfiles"],
  data = ["damlc/damlc.exe"],
)
""".format(name = ctx.attr.name),
    )
    return None

damlc_legacy = repository_rule(
    implementation = _damlc_legacy_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "os_name": attr.string(mandatory = False, default = os_name),
        "sha256": attr.string_dict(mandatory = True),
    },
)
