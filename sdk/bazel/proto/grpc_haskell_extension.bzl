load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:grpc_haskell.version.bzl",
    "GRPC_HASKELL_REV",
    "GRPC_HASKELL_SHA256",
)

_URLS = [
    "https://codeload.github.com/awakesecurity/gRPC-haskell/tar.gz/{}".format(GRPC_HASKELL_REV),
    "https://github.com/awakesecurity/gRPC-haskell/archive/{}.tar.gz".format(GRPC_HASKELL_REV),
]
_PATCHES = [
    # Includes MetadataMap in GRPCIOBadStatusCode for richer error reporting.
    "//bazel/patches:haskell/grpc-haskell-core-bad-status-meta.patch",
    # Stubs out C API types/functions removed in gRPC >= 1.74 (auth metadata
    # plugin, SSL credential helpers).  daml never calls these; see
    # bzlmigration/CRITICAL_CHANGES/01_grpc-haskell-c-api-compat.md
    "//bazel/patches:haskell/grpc-haskell-core-grpc-compat.patch",
]

def _impl(module_ctx):
    http_archive(
        name = "grpc_haskell_core",
        build_file = ":files/grpc_haskell_core.BUILD.bzl",
        patch_args = ["-p1"],
        patches = _PATCHES,
        sha256 = GRPC_HASKELL_SHA256,
        strip_prefix = "gRPC-haskell-{}/core".format(GRPC_HASKELL_REV),
        type = "tar.gz",
        urls = _URLS,
    )
    http_archive(
        name = "grpc_haskell_core_cbits",
        build_file = ":files/grpc_haskell_core_cbits.BUILD.bzl",
        patch_args = ["-p1"],
        patches = _PATCHES,
        sha256 = GRPC_HASKELL_SHA256,
        strip_prefix = "gRPC-haskell-{}/core".format(GRPC_HASKELL_REV),
        type = "tar.gz",
        urls = _URLS,
    )
    http_archive(
        name = "grpc_haskell",
        build_file = ":files/grpc_haskell.BUILD.bzl",
        sha256 = GRPC_HASKELL_SHA256,
        strip_prefix = "gRPC-haskell-{}".format(GRPC_HASKELL_REV),
        type = "tar.gz",
        urls = _URLS,
    )

grpc_haskell = module_extension(implementation = _impl)
