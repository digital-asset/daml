_MISSING = """`git` was not found on PATH.

The Daml rules apply the `patches` attribute with `git apply`, so a host git is
required. Install git, or put it on PATH, and re-run the build."""

def _host_git_repo_impl(rctx):
    git = rctx.which("git")
    if git == None:
        fail(_MISSING)
    rctx.file("BUILD.bazel", "")
    rctx.file("git.bzl", 'GIT_PATH = "{}"\n'.format(str(git).replace("\\", "/")))

_host_git_repo = repository_rule(
    implementation = _host_git_repo_impl,
    configure = True,
    local = True,
    environ = ["PATH"],
)

def _impl(_module_ctx):
    _host_git_repo(name = "host_git")

host_git = module_extension(implementation = _impl)
