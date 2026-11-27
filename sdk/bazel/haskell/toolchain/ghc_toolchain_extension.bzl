# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//bazel/platforms:platforms.bzl", "host_platform")
load(
    "//bazel/versions:ghc.version.bzl",
    "GHC_BINDISTS",
    "GHC_LLVM_BACKEND",
    "GHC_LLVM_BACKENDS",
    "GHC_VERSION",
)
load(
    "//bazel/versions:gnu_tools.version.bzl",
    "GMP_SHA256",
    "GMP_URLS",
    "GMP_VERSION",
    "NCURSES_LINUX_SHA256",
    "NCURSES_LINUX_URLS",
    "NCURSES_LINUX_VERSION",
    "NUMACTL_SHA256",
    "NUMACTL_VERSION",
)
load(":ghc_repository_rules.bzl", "ghc_bindist_repo", "ghc_llvm_backend_repo")

_URL = "https://downloads.haskell.org/~ghc/{v}/ghc-{v}-{triple}.tar.xz"

_LOCKFILES = {
    ("linux", "amd64"): Label("//bazel/haskell/ghc:pin/linux_amd64.lock.json"),
    ("linux", "aarch64"): Label("//bazel/haskell/ghc:pin/linux_aarch64.lock.json"),
    ("darwin", "amd64"): Label("//bazel/haskell/ghc:pin/darwin_amd64.lock.json"),
    ("darwin", "aarch64"): Label("//bazel/haskell/ghc:pin/darwin_aarch64.lock.json"),
    ("windows", "amd64"): Label("//bazel/haskell/ghc:pin/windows_amd64.lock.json"),
}

# The deb10 RTS records libnuma.so.1 as NEEDED and its `extra_libraries`
# emit -lnuma, but the bindist ships no libnuma. Declaring our hermetic copy on the
# rts import is what puts it in the runfiles/solib tree with a usable rpath for
# every target that links the RTS.
_VENDORED_SHARED_LIBRARIES = {
    ("linux", "amd64"): {
        "rts": ["@@//bazel/haskell/toolchain:libnuma.so.1"],
    },
    ("linux", "aarch64"): {
        "rts": ["@@//bazel/haskell/toolchain:libnuma.so.1"],
    },
}

def _ghc_bindist(key):
    bindist = GHC_BINDISTS.get(key)
    if bindist == None:
        fail("no GHC {} bindist for platform {}; supported: {}".format(
            GHC_VERSION,
            key,
            GHC_BINDISTS.keys(),
        ))
    lockfile = _LOCKFILES.get(key)
    if lockfile == None:
        fail("no GHC package pin for platform {}; supported: {}".format(key, _LOCKFILES.keys()))
    ghc_bindist_repo(
        name = "ghc_bindist",
        url = _URL.format(v = GHC_VERSION, triple = bindist["triple"]),
        sha256 = bindist["sha256"],
        strip_prefix = bindist["strip_prefix"],
        lockfile = lockfile,
        vendored_shared_libraries = _VENDORED_SHARED_LIBRARIES.get(key, {}),
        link_mingw_gmp_header = key == ("windows", "amd64"),
    )

def _ghc_llvm_backend(key):
    backend = GHC_LLVM_BACKENDS.get(key) if GHC_LLVM_BACKEND else None
    artifacts = backend["artifacts"] if backend else []
    ghc_llvm_backend_repo(
        name = "ghc_llvm_backend",
        urls = {a["output"]: a["url"] for a in artifacts},
        sha256 = {a["output"]: a["sha256"] for a in artifacts},
        shared_library = backend["shared_library"] if backend else "",
    )

def _ghc_toolchain_impl(module_ctx):
    key = host_platform(module_ctx).ghc_key
    _ghc_bindist(key)
    _ghc_llvm_backend(key)
    http_archive(
        name = "gmp",
        urls = GMP_URLS,
        sha256 = GMP_SHA256,
        strip_prefix = "gmp-{}".format(GMP_VERSION),
        build_file = ":files/gmp.BUILD.bzl",
        patches = [":files/gmp_handauthored.patch"],
        patch_args = ["-p1"],
    )

    http_archive(
        name = "ncurses",
        urls = NCURSES_LINUX_URLS,
        sha256 = NCURSES_LINUX_SHA256,
        strip_prefix = "ncurses-{}".format(NCURSES_LINUX_VERSION),
        build_file = ":files/ncurses.BUILD.bzl",
    )
    http_archive(
        name = "numactl",
        url = "https://github.com/numactl/numactl/releases/download/v{v}/numactl-{v}.tar.gz".format(v = NUMACTL_VERSION),
        sha256 = NUMACTL_SHA256,
        strip_prefix = "numactl-{}".format(NUMACTL_VERSION),
        build_file = ":files/numactl.BUILD.bzl",
    )

ghc_toolchain_extension = module_extension(
    implementation = _ghc_toolchain_impl,
    os_dependent = True,
    arch_dependent = True,
)
