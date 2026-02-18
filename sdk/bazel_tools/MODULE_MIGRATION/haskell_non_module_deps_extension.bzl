load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _get_cabal():
    http_archive(
        name = "Cabal",
        build_file = ":haskell/haskell_cabal.BUILD.bzl",
        sha256 = "b697b558558f351d2704e520e7dcb1f300cd77fea5677d4b2ee71d0b965a4fe9",
        strip_prefix = "cabal-ghc-9.4-paths-module-relocatable",
        urls = ["https://github.com/tweag/cabal/archive/refs/heads/ghc-9.4-paths-module-relocatable.zip"],
    )

def _get_lsp_types():
    LSP_TYPES_VERSION = "1.4.0.0"
    LSP_TYPES_SHA256 = "7ae8a3bad0e91d4a2af9b93e3ad207e3f4c3dace40d420e0592f6323ac93fb67"

    http_archive(
        name = "lsp-types",
        build_file = ":haskell/haskell_lsp_types.BUILD.bzl",
        patch_args = ["-p1"],
        patches = [
            "//bazel_tools:lsp-types-normalisation.patch",
            "//bazel_tools:lsp-types-expose-other-modules.patch",
        ],
        sha256 = LSP_TYPES_SHA256,
        strip_prefix = "lsp-types-{}".format(LSP_TYPES_VERSION),
        urls = ["http://hackage.haskell.org/package/lsp-types-{version}/lsp-types-{version}.tar.gz".format(version = LSP_TYPES_VERSION)],
    )

def _impl(module_ctx):
    _get_cabal()
    _get_lsp_types()

haskell_non_module_deps_extension = module_extension(implementation = _impl)
