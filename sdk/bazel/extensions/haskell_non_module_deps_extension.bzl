load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load(
    "//bazel_tools/ghc-lib:version.bzl",
    "GHC_LIB_PATCHES",
    "GHC_LIB_REPO_URL",
    "GHC_LIB_REV",
    "GHC_LIB_SHA256",
    "GHC_PATCHES",
    "GHC_REPO_URL",
    "GHC_REV",
)

def _get_cabal():
    http_archive(
        name = "Cabal",
        build_file = ":files/haskell/haskell_cabal.BUILD.bzl",
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

def _get_ghcide():
    GHCIDE_REV = "2914f9328dc549bd4918c3cf0fc008690a102d70"
    GHCIDE_SHA256 = "bc2bd2fbfbfb501e303b35fb4b2f503b122e4eb0aff34472f0e882d2289afc57"
    GHCIDE_LOCAL_PATH = None

    http_archive(
        name = "ghcide_ghc_lib",
        build_file = ":files/haskell/haskell_ghcide_ghc_lib.BUILD.bzl",
        patch_args = ["-p1"],
        patches = [
            "//bazel_tools:haskell-ghcide-binary-q.patch",
        ],
        sha256 = None if GHCIDE_LOCAL_PATH != None else GHCIDE_SHA256,
        strip_prefix = None if GHCIDE_LOCAL_PATH != None else "daml-ghcide-%s" % GHCIDE_REV,
        url = "file://" + GHCIDE_LOCAL_PATH if GHCIDE_LOCAL_PATH != None else "https://github.com/digital-asset/daml-ghcide/archive/%s.tar.gz" % GHCIDE_REV,
    )

def _get_ghc_lib_gen():
    http_archive(
        name = "ghc-lib-gen",
        url = "{}/archive/{}.tar.gz".format(GHC_LIB_REPO_URL, GHC_LIB_REV),
        sha256 = GHC_LIB_SHA256,
        strip_prefix = "ghc-lib-{}".format(GHC_LIB_REV),
        build_file = ":files/haskell/haskell_ghc_lib_gen.BUILD.bzl",
        patches = GHC_LIB_PATCHES,
        patch_args = ["-p1"],
    )

def _get_da_ghc():
    git_repository(
        name = "da-ghc",
        remote = GHC_REPO_URL,
        commit = GHC_REV,
        recursive_init_submodules = True,
        build_file = ":files/haskell/haskell_da_ghc.BUILD.bzl",
        shallow_since = "1639050525 +0100",
        patches = GHC_PATCHES,
        patch_args = ["-p1"],
    )

def _get_zip():
    ZIP_VERSION = "1.7.1"

    http_archive(
        name = "zip",
        build_file = ":files/haskell/haskell_zip.BUILD.bzl",
        patch_args = ["-p1"],
        patches = [
            "//bazel/patches:haskell/zip.patch",
        ],
        sha256 = "0d7f02bbdf6c49e9a33d2eca4b3d7644216a213590866dafdd2b47ddd38eb746",
        strip_prefix = "zip-{}".format(ZIP_VERSION),
        urls = ["http://hackage.haskell.org/package/zip-{version}/zip-{version}.tar.gz".format(version = ZIP_VERSION)],
    )

def _impl(module_ctx):
    _get_lsp_types()
    _get_ghcide()
    _get_ghc_lib_gen()
    _get_da_ghc()
    _get_zip()

haskell_non_module_deps_extension = module_extension(implementation = _impl)
