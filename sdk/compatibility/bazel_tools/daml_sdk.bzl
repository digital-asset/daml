# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_darwin", "is_darwin_amd64", "is_linux_intel", "is_windows", "os_arch", "os_name")
load("@daml//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew_tool_home", "dadew_where")
load("@io_bazel_rules_scala//scala:scala_cross_version.bzl", "default_maven_server_urls")
load("//bazel_tools:versions.bzl", "versions")
load("//:versions.bzl", "internal_sdk_versions")

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

def _daml_assistant_sdk_impl(ctx):
    # The Daml assistant will mark the installed SDK read-only.
    # This breaks Bazel horribly on Windows to the point where
    # even `bazel clean --expunge` fails because it cannot remove
    # the installed SDK. Therefore, we do not use the assistant to
    # install the SDK but instead simply extract the SDK to the right
    # location and set the symlink ourselves.
    out_dir = ctx.path("sdk").get_child("sdk").get_child(ctx.attr.version)

    internal_sdk_version = internal_sdk_versions.get(ctx.attr.version, default = ctx.attr.version)

    ctx.download_and_extract(
        output = out_dir,
        url =
            "https://github.com/digital-asset/daml/releases/download/v{}/daml-sdk-{}-{}-{}.tar.gz".format(ctx.attr.version, internal_sdk_version, os_name, os_arch),
        sha256 = ctx.attr.sdk_sha256[os_name],
        stripPrefix = "sdk-{}".format(internal_sdk_version),
    )
    sdk_checksum = ctx.attr.sdk_sha256[os_name]

    ctx.download(
        output = "daml-types.tgz",
        url = "https://registry.npmjs.org/@daml/types/-/types-{}.tgz".format(internal_sdk_version),
        sha256 = getattr(ctx.attr, "daml_types_sha256"),
    )

    ctx.symlink(out_dir.get_child("daml").get_child("daml" + (".exe" if is_windows else "")), "sdk/bin/daml")
    ctx.file(
        "sdk/daml-config.yaml",
        content =
            """
auto-install: false
update-check: never
""",
    )

    # Depending on all files as runfiles results in thousands of symlinks
    # which eventually results in us running out of inodes on CI.
    # By writing all checksums to a file and only depending on that we get
    # only one extra runfile while still making sure that things are properly
    # invalidated.
    ctx.file(
        "sdk/sdk/{version}/checksums".format(version = ctx.attr.version),
        # We don’t really care about the order here but find is non-deterministic
        # and having something fixed is clearly better for caching.
        content = sdk_checksum,
    )

    ctx.template(
        "daml.cc",
        Label("@compatibility//bazel_tools:daml.cc.tpl"),
        substitutions = {"{SDK_VERSION}": ctx.attr.version, "{ASSISTANT_PATH}": "sdk/bin/daml", "{DPM_HOME}": ""},
    )
    ctx.file(
        "BUILD",
        content =
            """
package(default_visibility = ["//visibility:public"])
cc_binary(
  name = "daml",
  srcs = ["daml.cc"],
  data = [":sdk/bin/daml", ":sdk/sdk/{version}/checksums"],
  deps = ["@bazel_tools//tools/cpp/runfiles:runfiles"],
)
exports_files(["daml-types.tgz"])
""".format(version = ctx.attr.version),
    )
    return None

_daml_assistant_sdk = repository_rule(
    implementation = _daml_assistant_sdk_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sdk_sha256": attr.string_dict(mandatory = True),
        "daml_types_sha256": attr.string(mandatory = True),
    },
)

def daml_assistant_sdk(version, **kwargs):
    _daml_assistant_sdk(
        name = "daml-sdk-{}".format(version),
        version = version,
        **kwargs
    )

def _dpm_sdk_impl(ctx):
    out_dir = ctx.path("sdk").get_child(ctx.attr.version)

    internal_sdk_version = internal_sdk_versions.get(ctx.attr.version, default = ctx.attr.version)

    arch = "amd64" if is_windows or is_linux_intel or is_darwin_amd64 else "arm64"

    # os_name for macos is "macos" rather than "darwin"
    os = "darwin" if is_darwin else os_name

    ctx.download_and_extract(
        output = out_dir,
        url =
            # arch must be amd64 or arm64. os_arch has x86_64 and aarch64
            "https://artifactregistry.googleapis.com/v1/projects/da-images/locations/europe/repositories/public-generic/files/dpm-sdk:{}:dpm-{}-{}-{}.tar.gz:download?alt=media".format(ctx.attr.version, ctx.attr.version, os, arch),
        sha256 = ctx.attr.sdk_sha256[os_name],
        stripPrefix = "{}-{}".format(os, arch),
        type = "tar.gz",
    )
    sdk_checksum = ctx.attr.sdk_sha256[os_name]

    ctx.download(
        output = "daml-types.tgz",
        url = "https://registry.npmjs.org/@daml/types/-/types-{}.tgz".format(internal_sdk_version),
        sha256 = getattr(ctx.attr, "daml_types_sha256"),
    )

    # Depending on all files as runfiles results in thousands of symlinks
    # which eventually results in us running out of inodes on CI.
    # By writing all checksums to a file and only depending on that we get
    # only one extra runfile while still making sure that things are properly
    # invalidated.
    ctx.file(
        "sdk/{version}/checksums".format(version = ctx.attr.version),
        # We don’t really care about the order here but find is non-deterministic
        # and having something fixed is clearly better for caching.
        content = sdk_checksum,
    )

    assistant_path = "sdk/{version}/bin/dpm{exe}".format(version = ctx.attr.version, exe = ".exe" if is_windows else "")

    ctx.execute([assistant_path, "bootstrap", "{}".format(out_dir)], environment = {"DPM_HOME": "{}".format(out_dir)})

    ctx.template(
        "dpm.cc",
        Label("@compatibility//bazel_tools:daml.cc.tpl"),
        substitutions = {
            "{SDK_VERSION}": ctx.attr.version,
            "{ASSISTANT_PATH}": assistant_path,
            "{DPM_HOME}": "{}".format(out_dir),
        },
    )
    ctx.file(
        "BUILD",
        content =
            """
package(default_visibility = ["//visibility:public"])
cc_binary(
  name = "dpm",
  srcs = ["dpm.cc"],
  data = [":{assistant_path}", ":sdk/{version}/checksums"],
  deps = ["@bazel_tools//tools/cpp/runfiles:runfiles"],
)
alias(
    name = "daml",
    actual = ":dpm",
)
exports_files(["daml-types.tgz"])
""".format(version = ctx.attr.version, assistant_path = assistant_path),
    )
    return None

_dpm_sdk = repository_rule(
    implementation = _dpm_sdk_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sdk_sha256": attr.string_dict(mandatory = True),
        "daml_types_sha256": attr.string(mandatory = True),
    },
)

def dpm_sdk(version, **kwargs):
    _dpm_sdk(
        name = "daml-sdk-{}".format(version),
        version = version,
        **kwargs
    )

def _dpm_head_sdk_impl(ctx):
    version = "0.0.0"
    out_dir = ctx.path("sdk").get_child(version)

    internal_sdk_version = internal_sdk_versions.get(version, default = version)

    ctx.extract(
        ctx.attr.release_tarball,
        output = out_dir,
        stripPrefix = "dpm-sdk-release-tarball",
    )
    sha256sum = "sha256sum"
    if is_windows:
        ps = ctx.which("powershell")
        dadew = dadew_where(ctx, ps)
        sha256sum = dadew_tool_home(dadew, "msys2") + "\\usr\\bin\\sha256sum.exe"

    exec_result = ctx.execute([sha256sum, ctx.path(ctx.attr.release_tarball)])
    if exec_result.return_code:
        fail("Error executing sha256sum: {stdout}\n{stderr}".format(stdout = exec_result.stdout, stderr = exec_result.stderr))
    sdk_checksum = exec_result.stdout.strip()

    ctx.symlink(ctx.attr.daml_types_tarball, "daml-types.tgz")

    # Depending on all files as runfiles results in thousands of symlinks
    # which eventually results in us running out of inodes on CI.
    # By writing all checksums to a file and only depending on that we get
    # only one extra runfile while still making sure that things are properly
    # invalidated.
    ctx.file(
        "sdk/{version}/checksums".format(version = version),
        # We don’t really care about the order here but find is non-deterministic
        # and having something fixed is clearly better for caching.
        content = sdk_checksum,
    )

    assistant_path = "sdk/{version}/bin/dpm{exe}".format(version = version, exe = ".cmd" if is_windows else "")

    ctx.template(
        "dpm.cc",
        Label("@compatibility//bazel_tools:daml.cc.tpl"),
        substitutions = {
            "{SDK_VERSION}": version,
            "{ASSISTANT_PATH}": assistant_path,
            "{DPM_HOME}": "",
        },
    )
    ctx.file(
        "BUILD",
        content =
            """
package(default_visibility = ["//visibility:public"])
cc_binary(
  name = "dpm",
  srcs = ["dpm.cc"],
  data = [":{assistant_path}", ":sdk/{version}/checksums"],
  deps = ["@bazel_tools//tools/cpp/runfiles:runfiles"],
)
alias(
    name = "daml",
    actual = ":dpm",
)
exports_files(["daml-types.tgz"])
""".format(version = version, assistant_path = assistant_path),
    )
    return None

_dpm_head_sdk = repository_rule(
    implementation = _dpm_head_sdk_impl,
    attrs = {
        "release_tarball": attr.label(allow_single_file = True, mandatory = True),
        "daml_types_tarball": attr.label(allow_single_file = True, mandatory = True),
    },
)

def dpm_head_sdk(**kwargs):
    _dpm_head_sdk(
        name = "daml-sdk-0.0.0",
        **kwargs
    )
