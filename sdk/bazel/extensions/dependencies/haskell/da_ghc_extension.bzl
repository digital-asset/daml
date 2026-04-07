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

def _get_ghc_lib_gen():
    http_archive(
        name = "ghc-lib-gen",
        url = "{}/archive/{}.tar.gz".format(GHC_LIB_REPO_URL, GHC_LIB_REV),
        sha256 = GHC_LIB_SHA256,
        strip_prefix = "ghc-lib-{}".format(GHC_LIB_REV),
        build_file = ":files/haskell_ghc_lib_gen.BUILD.bzl",
        patches = ["//bazel/patches:haskell/ghc_lib_no_stack.patch"],
        patch_args = ["-p1"],
    )

def _get_da_ghc():
    git_repository(
        name = "da-ghc",
        remote = GHC_REPO_URL,
        commit = GHC_REV,
        recursive_init_submodules = True,
        build_file = ":files/haskell_da_ghc.BUILD.bzl",
        shallow_since = "1771323697 +0100",
        patch_args = ["-p1"],
    )

def _impl(module_ctx):
    _get_ghc_lib_gen()
    _get_da_ghc()

da_ghc = module_extension(implementation = _impl)
