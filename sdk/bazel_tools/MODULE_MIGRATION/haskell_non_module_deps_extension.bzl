load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def _get_cabal(module_ctx):
    http_archive(
        name = "Cabal",
        build_file = ":haskell/haskell_cabal.BUILD.bzl",
        sha256 = "b697b558558f351d2704e520e7dcb1f300cd77fea5677d4b2ee71d0b965a4fe9",
        strip_prefix = "cabal-ghc-9.4-paths-module-relocatable",
        urls = ["https://github.com/tweag/cabal/archive/refs/heads/ghc-9.4-paths-module-relocatable.zip"],
    )

def _impl(module_ctx):
    _get_cabal(module_ctx)

haskell_non_module_deps_extension = module_extension(implementation = _impl)
