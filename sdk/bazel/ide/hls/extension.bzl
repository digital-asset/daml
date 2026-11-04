# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel/versions:ghc.version.bzl", "GHC_VERSION")
load("//bazel/versions:hls.version.bzl", "HLS_BINDISTS", "HLS_SOURCE", "HLS_SOURCE_PLATFORMS", "HLS_URL", "HLS_VERSION")

_BUILD_FILE = """\
filegroup(
    name = "binary",
    srcs = ["{root}/bin/{binary}"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "libraries",
    srcs = glob(["{root}/lib/{ghc_version}/**"]),
    visibility = ["//visibility:public"],
)
"""

_BINARY_PREFIX = "haskell-language-server-"

def _only_entry(rctx, directory):
    entries = rctx.path(directory).readdir()
    if len(entries) != 1:
        fail("expected a single top-level directory in {}, found {}".format(
            rctx.attr.url,
            [entry.basename for entry in entries],
        ))
    return entries[0].basename

def _hls_bindist_impl(rctx):
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        output = "dist",
    )
    root = "dist/" + _only_entry(rctx, "dist")
    binary = _BINARY_PREFIX + rctx.attr.ghc_version
    if not rctx.path("{}/bin/{}".format(root, binary)).exists:
        fail("{} does not provide {}".format(rctx.attr.url, binary))
    for entry in rctx.path(root + "/bin").readdir():
        if entry.basename == binary:
            continue
        other_ghc_version = entry.basename[len(_BINARY_PREFIX):] if entry.basename.startswith(_BINARY_PREFIX) else ""
        if other_ghc_version[:1].isdigit():
            rctx.delete("{}/lib/{}".format(root, other_ghc_version))
        rctx.delete(entry)
    rctx.file("BUILD.bazel", _BUILD_FILE.format(
        root = root,
        binary = binary,
        ghc_version = rctx.attr.ghc_version,
    ))

hls_bindist = repository_rule(
    implementation = _hls_bindist_impl,
    attrs = {
        "ghc_version": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "url": attr.string(mandatory = True),
    },
)

_SOURCE_BUILD_FILE = """\
load("{source_bzl}", "hls_source_build")

filegroup(
    name = "srcs",
    srcs = glob(["**"], exclude = ["BUILD.bazel", "REPO.bazel"]),
)

hls_source_build(
    name = "binary",
    srcs = ":srcs",
    cabal_project = "cabal.project",
    ghc_version = "{ghc_version}",
    tags = ["manual"],
    target_compatible_with = ["@platforms//os:macos"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "libraries",
    srcs = [":binary"],
    output_group = "libraries",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
"""

def _hls_source_impl(rctx):
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        stripPrefix = rctx.attr.strip_prefix,
    )
    rctx.file("BUILD.bazel", _SOURCE_BUILD_FILE.format(
        source_bzl = str(Label("//bazel/ide/hls:source.bzl")),
        ghc_version = rctx.attr.ghc_version,
    ))

hls_source = repository_rule(
    implementation = _hls_source_impl,
    attrs = {
        "ghc_version": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "strip_prefix": attr.string(mandatory = True),
        "url": attr.string(mandatory = True),
    },
)

def _hls_impl(module_ctx):
    for (os, cpu), bindist in HLS_BINDISTS.items():
        hls_bindist(
            name = "hls_{}_{}".format(os, cpu),
            ghc_version = GHC_VERSION,
            sha256 = bindist["sha256"],
            url = HLS_URL.format(version = HLS_VERSION, triple = bindist["triple"]),
        )
    for os, cpu in HLS_SOURCE_PLATFORMS:
        hls_source(
            name = "hls_{}_{}".format(os, cpu),
            ghc_version = GHC_VERSION,
            sha256 = HLS_SOURCE["sha256"],
            strip_prefix = HLS_SOURCE["strip_prefix"],
            url = HLS_SOURCE["url"],
        )

hls = module_extension(implementation = _hls_impl)
