# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

_UNPACK_DIR = "bindist_unpacked"

_IMPORT_TEMPLATE = """\
haskell_import(
    name = {name},
    id = {id},
    version = {version},
    deps = {deps},
    linkopts = {linkopts},
    static_libraries = glob([{subdir} + "/*.a"], exclude = [{subdir} + "/*_p.a"], allow_empty = True),
    static_profiling_libraries = glob([{subdir} + "/*_p.a"], allow_empty = True),
    shared_libraries = glob([{subdir} + "/*.so", {subdir} + "/*.so.*"], allow_empty = True) + {vendored_shared_libraries},
    hdrs = [],
    includes = [],
    visibility = ["//visibility:public"],
)
"""

def _haskell_import(pkg, vendored_shared_libraries):
    subdir = "{}/{}".format(_UNPACK_DIR, pkg["build_subdir"])
    return _IMPORT_TEMPLATE.format(
        name = repr(pkg["name"]),
        id = repr(pkg["id"]),
        version = repr(pkg["version"]),
        deps = repr(pkg["deps"]),
        linkopts = repr(["-l" + lib for lib in pkg["extra_libraries"]]),
        subdir = repr(subdir),
        vendored_shared_libraries = repr(vendored_shared_libraries.get(pkg["name"], [])),
    )

def _ghc_bindist_repo_impl(rctx):
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        type = "tar.xz",
        stripPrefix = rctx.attr.strip_prefix,
        output = _UNPACK_DIR,
    )

    if rctx.attr.link_mingw_gmp_header:
        rctx.symlink(
            "{}/mingw/include/gmp.h".format(_UNPACK_DIR),
            "bindist_gmp_include/gmp.h",
        )

    lock = json.decode(rctx.read(rctx.path(rctx.attr.lockfile)))
    packages = lock.get("packages", [])
    imports = "\n".join([
        _haskell_import(p, rctx.attr.vendored_shared_libraries)
        for p in packages
    ])

    rctx.file(
        "libraries.bzl",
        content = "# Generated from the committed GHC package pin.\n" +
                  "TOOLCHAIN_LIBRARIES = {}\n".format(
                      repr(["@ghc_bindist//:" + p["name"] for p in packages]),
                  ),
        executable = False,
    )

    rctx.file(
        "BUILD.bazel",
        content = """\
load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_haskell//haskell:defs.bzl", "haskell_import")

package(default_visibility = ["//visibility:public"])

{imports}

filegroup(
    name = "bindist_srcs",
    srcs = glob(["{unpack}/**"]),
)

filegroup(
    name = "bindist_configure",
    srcs = glob(["{unpack}/configure"], allow_empty = True),
)

cc_library(
    name = "bindist_gmp",
    srcs = glob(["{unpack}/mingw/lib/libgmp.a"], allow_empty = True),
    hdrs = glob(["bindist_gmp_include/gmp.h"], allow_empty = True),
    includes = ["bindist_gmp_include"],
)

filegroup(
    name = "bindist_gmp_libs",
    srcs = glob(["{unpack}/mingw/lib/libgmp*"], allow_empty = True),
)
""".format(
            imports = imports,
            unpack = _UNPACK_DIR,
        ),
        executable = False,
    )

ghc_bindist_repo = repository_rule(
    implementation = _ghc_bindist_repo_impl,
    attrs = {
        "link_mingw_gmp_header": attr.bool(
            doc = "Expose the bindist's mingw gmp.h as `bindist_gmp_include/gmp.h` (Windows bindist only).",
        ),
        "lockfile": attr.label(
            mandatory = True,
            doc = "Committed per-platform package pin; regenerate with `bazel run //bazel/haskell/ghc:ghc_packages.pin`.",
        ),
        "sha256": attr.string(mandatory = True),
        "strip_prefix": attr.string(mandatory = True),
        "url": attr.string(mandatory = True),
        "vendored_shared_libraries": attr.string_list_dict(
            doc = "Package name to extra shared-library labels added to its `haskell_import`.",
        ),
    },
)

def _ghc_llvm_backend_repo_impl(rctx):
    for output, url in rctx.attr.urls.items():
        rctx.download_and_extract(
            url = url,
            sha256 = rctx.attr.sha256[output],
            type = "tar.bz2",
            output = output,
        )
    if rctx.attr.urls:
        srcs = '"tools/bin/opt", "tools/bin/llc", "lib/lib/{}"'.format(rctx.attr.shared_library)
    else:
        srcs = ""
    rctx.file("BUILD.bazel", """\
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "backend",
    srcs = [{srcs}],
)
""".format(srcs = srcs))

ghc_llvm_backend_repo = repository_rule(
    implementation = _ghc_llvm_backend_repo_impl,
    attrs = {
        "sha256": attr.string_dict(doc = "Output directory to archive sha256."),
        "shared_library": attr.string(doc = "libLLVM file name under `lib/lib`."),
        "urls": attr.string_dict(doc = "Output directory to archive URL; empty means no backend."),
    },
)
