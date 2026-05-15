# Custom `git_repository`-style rule used in this repo.
#
# Why not the stock `@bazel_tools//tools/build_defs/repo:git.bzl`:
#  * `git_repository` runs `git submodule update --init --recursive --checkout
#    --force` **serially** -- there is no `--jobs` flag and no public attribute
#    to add one (see bazelbuild/bazel#10298 and friends).
#  * Every `ctx.execute` it does is bound to the **default 600 s** repository
#    timeout. There is no `timeout` attribute on `git_repository`, and the
#    internal calls in `git_worker.bzl` do not forward one.
#  * On `digital-asset/ghc` (~30 submodules under `gitlab.haskell.org`) the
#    serial init regularly exceeds 600 s, leaving a partial
#    `.git/modules/<name>/` that breaks subsequent retries with
#    `fatal: Unable to find current revision in submodule path '...'`.
#
# This rule reproduces the parts of `git_worker.bzl` we need (`init` / `fetch`
# / `reset` / `clean`) verbatim, then calls `git submodule update` directly via
# `ctx.execute(..., timeout = submodule_timeout)` with `--jobs` exposed as an
# attribute. Patches/`patch_cmds` are delegated to the same
# `@bazel_tools//tools/build_defs/repo:utils.bzl#patch` helper that
# `git_repository` uses, so existing semantics carry over.

load(
    "@bazel_tools//tools/build_defs/repo:utils.bzl",
    "patch",
    "update_attrs",
    "workspace_and_buildfile",
)

_DEFAULT_GIT_TIMEOUT = 600
_DEFAULT_SUBMODULE_TIMEOUT = 1800
_DEFAULT_SUBMODULE_JOBS = 8

def _git(ctx, args, *, timeout, cwd = None, check = True):
    # "core.fsmonitor=false" mirrors `git_worker.bzl`: it stops git from
    # spawning a long-lived fsmonitor that can hang the clone.
    cmd = ["git", "-c", "core.fsmonitor=false"] + args
    st = ctx.execute(
        cmd,
        environment = ctx.os.environ,
        timeout = timeout,
        working_directory = str(cwd) if cwd else "",
    )
    if check and st.return_code != 0:
        fail("error running `git {}` while fetching @{}:\nstdout:\n{}\nstderr:\n{}".format(
            " ".join(args),
            ctx.name,
            st.stdout,
            st.stderr,
        ))
    return st

def _impl(ctx):
    if ctx.attr.build_file and ctx.attr.build_file_content:
        fail("Only one of build_file and build_file_content can be provided.")

    root = ctx.path(".")
    ctx.delete(root)

    git_timeout = ctx.attr.git_timeout
    submodule_timeout = ctx.attr.submodule_timeout

    _git(ctx, ["init", str(root)], timeout = git_timeout)
    _git(ctx, ["remote", "add", "origin", ctx.attr.remote], timeout = git_timeout, cwd = root)

    ctx.report_progress("Fetching {} (shallow) from {}".format(ctx.attr.commit, ctx.attr.remote))
    shallow = _git(
        ctx,
        ["fetch", "--depth=1", "origin", ctx.attr.commit],
        timeout = git_timeout,
        cwd = root,
        check = False,
    )
    if shallow.return_code != 0:
        ctx.report_progress("Shallow fetch failed; fetching full history")
        _git(
            ctx,
            [
                "fetch",
                "origin",
                "refs/heads/*:refs/remotes/origin/*",
                "refs/tags/*:refs/tags/*",
            ],
            timeout = git_timeout,
            cwd = root,
        )

    _git(ctx, ["reset", "--hard", ctx.attr.commit], timeout = git_timeout, cwd = root)
    _git(ctx, ["clean", "-xdf"], timeout = git_timeout, cwd = root)

    if ctx.attr.recursive_init_submodules or ctx.attr.init_submodules:
        recursive_args = ["--recursive"] if ctx.attr.recursive_init_submodules else []
        ctx.report_progress("Updating submodules with --jobs {} (timeout {}s)".format(
            ctx.attr.submodule_jobs,
            submodule_timeout,
        ))
        _git(
            ctx,
            [
                # `protocol.file.allow=always` is needed since git 2.38.1 to let
                # the submodule machinery clone via the `file://` transport
                # (bazelbuild/bazel#17040), matching `git_worker.bzl`.
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "update",
                "--init",
                "--checkout",
                "--force",
                "--jobs",
                str(ctx.attr.submodule_jobs),
            ] + recursive_args,
            timeout = submodule_timeout,
            cwd = root,
        )

    workspace_and_buildfile(ctx)
    patch(ctx)

    ctx.delete(root.get_child(".git"))

    return update_attrs(ctx.attr, _common_attrs.keys(), {"commit": ctx.attr.commit})

_common_attrs = {
    "remote": attr.string(
        mandatory = True,
        doc = "URI of the remote git repository.",
    ),
    "commit": attr.string(
        mandatory = True,
        doc = "Commit SHA to check out.",
    ),
    "init_submodules": attr.bool(
        default = False,
        doc = "Initialize top-level submodules. Ignored when `recursive_init_submodules` is True.",
    ),
    "recursive_init_submodules": attr.bool(
        default = True,
        doc = "Initialize submodules recursively (default).",
    ),
    "submodule_jobs": attr.int(
        default = _DEFAULT_SUBMODULE_JOBS,
        doc = "Value passed as `--jobs` to `git submodule update`.",
    ),
    "submodule_timeout": attr.int(
        default = _DEFAULT_SUBMODULE_TIMEOUT,
        doc = "Timeout (in seconds) for the single `git submodule update` call.",
    ),
    "git_timeout": attr.int(
        default = _DEFAULT_GIT_TIMEOUT,
        doc = "Timeout (in seconds) for each non-submodule `git` invocation.",
    ),
    "build_file": attr.label(
        allow_single_file = True,
        doc = "Optional BUILD file to drop into the repo root.",
    ),
    "build_file_content": attr.string(
        doc = "Optional inline BUILD file content. Mutually exclusive with `build_file`.",
    ),
    "patches": attr.label_list(
        default = [],
        doc = "Patches to apply (delegated to `@bazel_tools//tools/build_defs/repo:utils.bzl#patch`).",
    ),
    "patch_tool": attr.string(default = ""),
    "patch_args": attr.string_list(default = ["-p0"]),
    "patch_cmds": attr.string_list(default = []),
    "patch_cmds_win": attr.string_list(default = []),
}

da_git_repository = repository_rule(
    implementation = _impl,
    attrs = _common_attrs,
    doc = """Digital Asset variant of `@bazel_tools//tools/build_defs/repo:git.bzl#git_repository`.

It mirrors the stock rule's behavior (shallow fetch, reset, clean, patches,
`patch_cmds`) but adds two knobs needed by the GHC fork and any future
submodule-heavy git dependency in this repo:

  * `submodule_jobs` -- value passed as `--jobs` to `git submodule update`,
    so the recursive init runs in parallel instead of serially.
  * `submodule_timeout` / `git_timeout` -- explicit per-call timeouts for
    `ctx.execute`, since `git_repository` is hardcoded to the 600 s default
    and provides no way to raise it.

Use this whenever a git dep risks exceeding 600 s in any single git
invocation (large clone, many submodules, slow remote).""",
)
