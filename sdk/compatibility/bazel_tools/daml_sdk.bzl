# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load(
    "//compatibility:versions.bzl",
    "daml_assistant_versions",
    "dpm_versions",
    "internal_sdk_versions",
    "version_sha256s",
)

_WRAPPER = Label("//compatibility/bazel_tools:daml_wrapper.cc")

def _host(rctx):
    os = rctx.os.name.lower()
    arch = rctx.os.arch.lower()
    if arch not in ["amd64", "x86_64"]:
        fail("Compatibility SDKs are only pinned for x86_64, this host is {}/{}.".format(os, arch))
    if os.startswith("linux"):
        return struct(name = "linux", dpm_os = "linux", is_windows = False)
    if os.startswith("mac"):
        return struct(name = "macos", dpm_os = "darwin", is_windows = False)
    if os.startswith("windows"):
        return struct(name = "windows", dpm_os = "windows", is_windows = True)
    fail("Unsupported host OS for compatibility SDKs: {}".format(os))

def _string_define(name, value):
    return '{}=\\"{}\\"'.format(name, value)

def _write_wrapper(rctx, assistant_path, checksums_path, dpm_home_marker = None):
    defines = [
        _string_define("COMPAT_ASSISTANT_PATH", assistant_path),
        _string_define("COMPAT_JAVABASE", "$(JAVABASE)"),
    ]
    if dpm_home_marker:
        defines.append(_string_define("COMPAT_DPM_HOME_MARKER", dpm_home_marker))
    binary_name = "dpm" if dpm_home_marker else "daml"
    rctx.symlink(_WRAPPER, "daml_wrapper.cc")
    rctx.file("BUILD.bazel", """
load("@rules_cc//cc:cc_binary.bzl", "cc_binary")

package(default_visibility = ["//visibility:public"])
{daml_alias}
cc_binary(
    name = {binary_name},
    srcs = ["daml_wrapper.cc"],
    data = [
        {assistant},
        {checksums},
        "@rules_java//toolchains:current_java_runtime",
    ],
    local_defines = {defines},
    toolchains = ["@rules_java//toolchains:current_java_runtime"],
    deps = ["@bazel_tools//tools/cpp/runfiles"],
)
""".format(
        binary_name = repr(binary_name),
        daml_alias = "" if binary_name == "daml" else '\nalias(name = "daml", actual = ":dpm")\n',
        assistant = repr(":" + assistant_path),
        checksums = repr(":" + checksums_path),
        defines = repr(defines),
    ))

def _daml_assistant_sdk_impl(rctx):
    host = _host(rctx)
    version = rctx.attr.version
    internal_version = internal_sdk_versions.get(version, version)
    out_dir = rctx.path("sdk").get_child("sdk").get_child(version)
    rctx.download_and_extract(
        output = out_dir,
        url = "https://github.com/digital-asset/daml/releases/download/v{}/daml-sdk-{}-{}-x86_64.tar.gz".format(version, internal_version, host.name),
        sha256 = rctx.attr.sdk_sha256[host.name],
        stripPrefix = "sdk-{}".format(internal_version),
    )
    rctx.symlink(out_dir.get_child("daml").get_child("daml" + (".exe" if host.is_windows else "")), "sdk/bin/daml")
    rctx.file("sdk/daml-config.yaml", "auto-install: false\nupdate-check: never\n")
    checksums_path = "sdk/sdk/{}/checksums".format(version)
    rctx.file(checksums_path, rctx.attr.sdk_sha256[host.name])
    _write_wrapper(
        rctx,
        assistant_path = "sdk/bin/daml",
        checksums_path = checksums_path,
    )

_daml_assistant_sdk = repository_rule(
    implementation = _daml_assistant_sdk_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sdk_sha256": attr.string_dict(mandatory = True),
    },
)

def _dpm_sdk_impl(rctx):
    host = _host(rctx)
    version = rctx.attr.version
    out_dir = rctx.path("sdk").get_child(version)
    extension = "zip" if host.is_windows else "tar.gz"
    archive = "dpm-release.{}".format(extension)
    rctx.download(
        output = archive,
        url = "https://artifactregistry.googleapis.com/v1/projects/da-images/locations/europe/repositories/public-generic/files/dpm-sdk:{v}:dpm-{v}-{os}-amd64.{ext}:download?alt=media".format(
            v = version,
            os = host.dpm_os,
            ext = extension,
        ),
        sha256 = rctx.attr.sdk_sha256[host.name],
    )
    rctx.extract(
        archive = archive,
        output = out_dir,
        stripPrefix = "{}-amd64".format(host.dpm_os),
    )
    rctx.delete(archive)
    assistant_path = "sdk/{}/bin/dpm{}".format(version, ".exe" if host.is_windows else "")
    home = rctx.path("bootstrap-home")
    rctx.file("bootstrap-home/.keep", "")
    result = rctx.execute(
        [assistant_path, "bootstrap", str(out_dir)],
        environment = {
            "DPM_HOME": str(out_dir),
            "HOME": str(home),
            "USERPROFILE": str(home),
        },
        quiet = False,
        timeout = 1800,
    )
    if result.return_code:
        fail("dpm bootstrap failed for {}:\n{}\n{}".format(version, result.stdout, result.stderr))
    checksums_path = "sdk/{}/checksums".format(version)
    rctx.file(checksums_path, rctx.attr.sdk_sha256[host.name])
    _write_wrapper(
        rctx,
        assistant_path = assistant_path,
        checksums_path = checksums_path,
        dpm_home_marker = checksums_path,
    )

_dpm_sdk = repository_rule(
    implementation = _dpm_sdk_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sdk_sha256": attr.string_dict(mandatory = True),
    },
)

def _compat_sdks_hub_impl(rctx):
    rctx.file("BUILD.bazel", "\n".join([
        'package(default_visibility = ["//visibility:public"])',
    ] + [
        'alias(name = "daml-{v}", actual = "@daml-sdk-{v}//:daml")'.format(v = v)
        for v in rctx.attr.versions
    ]) + "\n")

_compat_sdks_hub = repository_rule(
    implementation = _compat_sdks_hub_impl,
    attrs = {"versions": attr.string_list(mandatory = True)},
)

def _compat_sdks_impl(module_ctx):
    for version in daml_assistant_versions:
        _daml_assistant_sdk(
            name = "daml-sdk-{}".format(version),
            version = version,
            sdk_sha256 = version_sha256s[version],
        )
    for version in dpm_versions:
        _dpm_sdk(
            name = "daml-sdk-{}".format(version),
            version = version,
            sdk_sha256 = version_sha256s[version],
        )
    _compat_sdks_hub(
        name = "compat_sdks",
        versions = daml_assistant_versions + dpm_versions,
    )
    return module_ctx.extension_metadata(reproducible = True)

compat_sdks = module_extension(implementation = _compat_sdks_impl)
