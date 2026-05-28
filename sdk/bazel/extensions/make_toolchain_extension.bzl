"""Module extension that downloads and builds GNU make from source at fetch time.

The output is a single statically-linked `bin/make` binary that
*repository rules* can use to drive `make`-based builds without
depending on a host `make` on the agent's `PATH`. Consumers resolve it
via `ctx.path(Label("@hermetic_make_linux_amd64//:bin/make"))` and
either `exec` it directly or prepend its directory to a child process's
`PATH`.

# Why fetch-time and not action-time

The other GNU-tools extensions in this directory (m4, autoconf, ncurses,
bzip2, ...) follow an action-time pattern: a thin `http_archive` + a
genrule in `files/<tool>.BUILD.bzl` that runs `./configure && make -j`
under the Bazel sandbox using `@rules_cc//cc:current_cc_toolchain`. That
pattern does not work here because:

  - The consumer is a *repository rule*, which runs at fetch time,
    before any Bazel action. A genrule output lives at `bazel-bin/.../`
    and is unreachable from `ctx.path(Label(...))` inside another
    repository rule.
  - The action-time genrules elsewhere in this tree shell out to host
    `make -j` for the inner build, which reintroduces the host
    dependency we want to remove.

Building from source at fetch time, with GNU make's bundled `build.sh`
bootstrap script, breaks the chicken-and-egg cycle: `build.sh` compiles
make using only a C compiler (no host make required).

# Static linking + --disable-nls

Default dynamic linkage against a newer glibc sysroot makes
the resulting binary unrunnable on hosts whose glibc is older than the
sysroot's. `CFLAGS=-static -O2 LDFLAGS=-static` plus `--disable-nls`
produces a fully self-contained ~1.6 MB ELF that runs on any Linux
kernel >= 3.2.0 regardless of the host's glibc version. Build overhead
is ~10 s of wall time on top of the ~2 MB tarball download.

# Scope

Linux x86_64 only. The binary is consumed by Linux-only repository
rules; macOS and Windows fall through to host `make`.
"""

load(
    "//bazel/versions:gnu_tools.version.bzl",
    "MAKE_SHA256",
    "MAKE_VERSION",
)

_PLATFORMS = {
    "linux_amd64": struct(
        repo_name = "hermetic_make_linux_amd64",
        url = "https://ftp.gnu.org/gnu/make/make-{}.tar.gz".format(MAKE_VERSION),
        sha256 = MAKE_SHA256,
        strip_prefix = "make-{}".format(MAKE_VERSION),
    ),
}

_BUILD_TPL = """\
filegroup(
    name = "all_files",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

exports_files(
    ["bin/make"],
    visibility = ["//visibility:public"],
)
"""

def _fail_with_output(label, result):
    fail("{} failed (rc={}):\n--- stdout ---\n{}\n--- stderr ---\n{}".format(
        label,
        result.return_code,
        result.stdout,
        result.stderr,
    ))

def _hermetic_make_repo_impl(rctx):
    rctx.download_and_extract(
        url = rctx.attr.url,
        sha256 = rctx.attr.sha256,
        stripPrefix = rctx.attr.strip_prefix,
    )

    cc_path = rctx.which("cc")
    if not cc_path:
        fail("make_toolchain_extension requires `cc` on PATH during repository rule execution.")

    # Inherit the host PATH for `sh`, `mkdir`, `mv`, and the small set
    # of POSIX utilities that GNU make's `./configure` and `./build.sh`
    # invoke. Hermeticity for those utilities is out of scope for this
    # extension.
    system_path = rctx.os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
    build_env = {
        "CC": str(cc_path),
        "PATH": system_path,
        "CFLAGS": "-static -O2",
        "LDFLAGS": "-static",
    }

    # Step 1: autoconf-driven configure. This is the first autoconf
    # invocation in the tree that depends on `unprefixed/`, so it
    # doubles as a regression test for that directory's contract.
    configure_result = rctx.execute(
        ["./configure", "--disable-dependency-tracking", "--disable-nls"],
        environment = build_env,
        timeout = 600,
    )
    if configure_result.return_code != 0:
        _fail_with_output("./configure", configure_result)

    # Step 2: bootstrap make from C sources without needing make itself.
    # `build.sh` ships in every GNU make release for exactly this
    # scenario.
    build_result = rctx.execute(
        ["sh", "./build.sh"],
        environment = build_env,
        timeout = 600,
    )
    if build_result.return_code != 0:
        _fail_with_output("./build.sh", build_result)

    # Step 3: relocate the binary to a stable, conventional path. The
    # build leaves `make` at the source root; consumers expect
    # `@hermetic_make_linux_amd64//:bin/make` to mirror the expected
    # `bin/<tool>` layout.
    relocate_result = rctx.execute(
        ["sh", "-c", "mkdir -p bin && mv make bin/make"],
    )
    if relocate_result.return_code != 0:
        _fail_with_output("relocate make -> bin/make", relocate_result)

    rctx.file("BUILD.bazel", _BUILD_TPL)

_hermetic_make_repo = repository_rule(
    implementation = _hermetic_make_repo_impl,
    attrs = {
        "url": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "strip_prefix": attr.string(mandatory = True),
    },
)

def _impl(_module_ctx):
    for info in _PLATFORMS.values():
        _hermetic_make_repo(
            name = info.repo_name,
            url = info.url,
            sha256 = info.sha256,
            strip_prefix = info.strip_prefix,
        )

make_toolchain = module_extension(implementation = _impl)
