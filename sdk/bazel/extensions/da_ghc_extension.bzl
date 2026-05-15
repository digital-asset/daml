load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//bazel/rules:da_git_repository.bzl", "da_git_repository")
load(
    "//bazel/versions:da_ghc.version.bzl",
    "GHC_LIB_REPO_URL",
    "GHC_LIB_REV",
    "GHC_LIB_SHA256",
    "GHC_REPO_URL",
    "GHC_REV",
)

def _get_ghc_lib_gen():
    http_archive(
        name = "ghc-lib-gen",
        url = "{}/archive/{}.tar.gz".format(GHC_LIB_REPO_URL, GHC_LIB_REV),
        sha256 = GHC_LIB_SHA256,
        strip_prefix = "ghc-lib-{}".format(GHC_LIB_REV),
        build_file = ":files/ghc_lib_gen.BUILD.bzl",
        patches = ["//bazel/patches:haskell/ghc_lib_no_stack.patch"],
        patch_args = ["-p1"],
    )

def _get_da_ghc():
    # `@bazel_tools//tools/build_defs/repo:git.bzl#git_repository` runs the
    # recursive `git submodule update` serially with the default 600 s
    # `ctx.execute` timeout, and `digital-asset/ghc` (~30 submodules on
    # `gitlab.haskell.org`) regularly exceeds that. We use a local custom
    # rule that exposes `--jobs` and a per-call `timeout`; see
    # `da_git_repository.bzl` for details.
    da_git_repository(
        name = "da-ghc",
        remote = GHC_REPO_URL,
        commit = GHC_REV,
        recursive_init_submodules = True,
        submodule_jobs = 8,
        submodule_timeout = 1800,
        build_file = ":files/da_ghc.BUILD.bzl",
        patch_args = ["-p1"],
    )

def _impl(module_ctx):
    _get_ghc_lib_gen()
    _get_da_ghc()

da_ghc = module_extension(implementation = _impl)
