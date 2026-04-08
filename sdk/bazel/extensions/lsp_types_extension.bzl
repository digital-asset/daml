load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel/versions:lsp_types.version.bzl",
    "LSP_TYPES_SHA256",
    "LSP_TYPES_VERSION",
)

def _impl(module_ctx):
    http_archive(
        name = "lsp-types",
        build_file = ":files/lsp_types.BUILD.bzl",
        patch_args = ["-p1"],
        patches = [
            "//bazel/patches:haskell/lsp_types_normalisation.patch",
            "//bazel/patches:haskell/lsp_types_expose_other_modules.patch",
        ],
        sha256 = LSP_TYPES_SHA256,
        strip_prefix = "lsp-types-{}".format(LSP_TYPES_VERSION),
        urls = ["http://hackage.haskell.org/package/lsp-types-{version}/lsp-types-{version}.tar.gz".format(version = LSP_TYPES_VERSION)],
    )

lsp_types = module_extension(implementation = _impl)
