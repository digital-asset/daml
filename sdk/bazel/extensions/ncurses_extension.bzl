"""Module extension that builds the hermetic ncurses 6 distribution at
repo-fetch time.

Why a custom repository_rule (instead of the original http_archive +
genrule shape):

The rules_haskell GHC 9.0.2 deb9 bindist's `ghc-pkg` is dynamically
linked against libtinfo.so.5.  When rules_haskell's `_ghc_bindist_impl`
runs `make install` at repo-fetch time it execs that `ghc-pkg`, and on
a host that ships only libtinfo.so.6 (Ubuntu 22.04, Azure pipelines
default, …) the install fails with `error while loading shared
libraries: libtinfo.so.5: cannot open shared object file`.  The bindist
patch (rules_haskell_hermetic_cc.patch) wants to point
`LD_LIBRARY_PATH` at the hermetic ncurses 6 build's
`lib/libtinfo.so.5` so the loader resolves without depending on a host
libtinfo5 package — but a `repository_ctx.path` call on a label can
only resolve files that exist on disk at fetch time, and the previous
http_archive + genrule design produced libtinfo.so.5 only at action
execution time.

This rule shifts the configure / make / make install steps into
`repository_ctx.execute` so libtinfo.so.5 is a real file by the time
other repository rules query its path.  Pattern mirrors
`gcc_toolchain_extension._gcc_toolchain_repo_impl` which already builds
the Bootlin GCC toolchain at fetch time for the same reason.

The single action-time tool we still need is `patchelf`, used to
rewrite the embedded SONAME of `lib/libtinfo.so` to `libtinfo.so` (so
haskeline's emitted `-ltinfo` DT_NEEDED entries resolve at runtime via
the hermetic ncurses build rather than a host libtinfo).  `@patchelf`
from BCR is a `cc_binary` target — it is only buildable at action time
— so the SONAME rewrite stays in a small genrule emitted from the
external BUILD.bazel template at
`sdk/bazel/extensions/files/ncurses.BUILD.bzl`.  Other consumers
(`@ncurses//:libs` for `damlc/BUILD.bazel`, and `@ncurses//:tinfo_cc_lib`
for haskeline's `stack_snapshot` `extra_deps`) keep their existing
shape.
"""

load(
    "//bazel/versions:gnu_tools.version.bzl",
    "NCURSES_SHA256",
    "NCURSES_VERSION",
)

# Shell snippet that runs after `make install` to (a) replace any
# symlinks under lib/ with regular file copies (Bazel sandboxing /
# remote-exec uploads handle real files more predictably) and (b)
# materialise filename-only versioned copies of libtinfow.so as
# libtinfo.so.5 / libtinfo.so.6 so the dynamic loader's filename-based
# search resolves them without needing the SONAME to match.
_POST_INSTALL_SH = """\
set -euo pipefail
cd lib
for f in *.so; do
    if [ -L "$f" ]; then
        target=$(readlink -f "$f")
        rm "$f"
        cp "$target" "$f"
    fi
done
for f in *.so.*; do
    if [ -L "$f" ]; then
        target=$(readlink -f "$f")
        rm "$f"
        cp "$target" "$f"
    fi
done
for lib in ncurses form menu panel; do
    cp lib${lib}w.so lib${lib}.so
done
cp libtinfow.so libtinfo.so.5
cp libtinfow.so libtinfo.so.6
"""

def _check(label, res):
    if res.return_code != 0:
        fail(
            "{} failed (exit {}):\nstdout:\n{}\nstderr:\n{}".format(
                label,
                res.return_code,
                res.stdout,
                res.stderr,
            ),
        )

def _ncurses_repo_impl(rctx):
    # Extract into `src/` rather than the repo root so the build tree
    # (src/lib/) and the install tree (lib/, set via --prefix below)
    # are distinct directories.  The previous in-tree layout caused
    # ncurses' install rules to invoke `/usr/bin/install ../lib/X X`
    # and fail with "are the same file" once build_dir == prefix.
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        stripPrefix = rctx.attr.strip_prefix,
        output = "src",
    )

    # Canonical bzlmod label form (`@@<root>~<extension>~<repo>`) is
    # required: this rule's repo is materialised as @@_main~ncurses~ncurses
    # and has no apparent-name mapping back to the root module's repos.
    # cc is the autoconf-friendly unprefixed shim from
    # gcc_toolchain_extension; ar is the binutils symlink in the same
    # `unprefixed/` farm.  Both exist as real files at fetch time
    # because gcc_toolchain_extension already runs at fetch time.
    cc = rctx.path(Label("@@_main~gcc_toolchain~hermetic_cc_linux_amd64//:unprefixed/cc"))
    ar = rctx.path(Label("@@_main~gcc_toolchain~hermetic_cc_linux_amd64//:unprefixed/ar"))
    unprefixed_dir = str(rctx.path(Label("@@_main~gcc_toolchain~hermetic_cc_linux_amd64//:unprefixed/gcc")).dirname)
    make = rctx.path(Label("@@_main~make_toolchain~hermetic_make_linux_amd64//:bin/make"))
    make_dir = str(make.dirname)
    system_path = rctx.os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")

    env = {
        "CC": str(cc),
        "AR": str(ar),
        # PATH must include `unprefixed/` (for autoconf's ar / ld /
        # ranlib probes) and the hermetic make's directory (so any
        # nested `make` invocations inside the ncurses Makefiles
        # resolve to the hermetic binary).  system_path is appended
        # last so the build keeps access to /bin/sh, awk, sed, tar,
        # which are not bundled by the hermetic toolchains.
        "PATH": "{}:{}:{}".format(unprefixed_dir, make_dir, system_path),
    }

    # Install into the repo root so the BUILD template's `lib/*.so`
    # globs resolve at the standard locations.  Source tree stays
    # isolated under `src/` to avoid the build-tree/install-tree
    # collision documented at the download_and_extract call above.
    prefix = str(rctx.path("."))

    _check(
        "ncurses configure",
        rctx.execute(
            [
                "./configure",
                "--prefix=" + prefix,
                "--with-shared",
                "--with-termlib",
                "--enable-widec",
                "--without-debug",
                "--without-ada",
                "--without-manpages",
                "--without-tests",
            ],
            environment = env,
            working_directory = "src",
            timeout = 600,
        ),
    )

    nproc_res = rctx.execute(["nproc"])
    nproc = nproc_res.stdout.strip() if nproc_res.return_code == 0 else "1"

    _check(
        "ncurses make",
        rctx.execute(
            [str(make), "-j" + nproc],
            environment = env,
            working_directory = "src",
            timeout = 1800,
        ),
    )

    _check(
        "ncurses make install",
        rctx.execute(
            [str(make), "install"],
            environment = env,
            working_directory = "src",
            timeout = 600,
        ),
    )

    _check(
        "ncurses lib materialisation",
        rctx.execute(
            ["bash", "-c", _POST_INSTALL_SH],
            environment = env,
            timeout = 60,
        ),
    )

    # Pull in the static BUILD template from the root module so it
    # gets normal IDE / buildifier coverage instead of living as an
    # escaped Starlark string here.  substitutions = {} because the
    # file has no placeholders -- the .so file set is fixed by
    # ncurses' configure flags and does not vary per platform.
    rctx.template(
        "BUILD.bazel",
        Label("//bazel:extensions/files/ncurses.BUILD.bzl"),
        substitutions = {},
        executable = False,
    )

_ncurses_repo = repository_rule(
    implementation = _ncurses_repo_impl,
    attrs = {
        "url": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "strip_prefix": attr.string(mandatory = True),
    },
)

def _impl(module_ctx):
    _ncurses_repo(
        name = "ncurses",
        url = "https://ftp.gnu.org/gnu/ncurses/ncurses-{}.tar.gz".format(NCURSES_VERSION),
        sha256 = NCURSES_SHA256,
        strip_prefix = "ncurses-{}".format(NCURSES_VERSION),
    )

ncurses = module_extension(implementation = _impl)
