load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("//bazel/native:hermetic_cc.bzl", "TOOLBIN_SNIPPET", "hermetic_cc_flags")

def _build_make_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    cc = hermetic_cc_flags(ctx, cc_toolchain)

    make_bin = ctx.outputs.make_binary
    configure = ctx.file.configure
    src_dir = configure.dirname

    command = """\
set -euo pipefail

EXECROOT="$PWD"
OUT="$EXECROOT/{out}"
SRC="$EXECROOT/{src_dir}"
CLANG="$EXECROOT/{compiler}"

# lld ships next to clang; the sandbox has no host ld.
export CC="$CLANG -fuse-ld=lld"
export CFLAGS="{cflags}"
export LDFLAGS="{ldflags}"
# configure's header probes use $CPPFLAGS, not $CFLAGS; without the sysroot here
# they mis-detect uid_t/gid_t, which then clash with the real glibc typedefs.
export CPPFLAGS="{cflags}"
{toolbin}
cd "$SRC"

# build.sh compiles make without needing make itself.
./configure --disable-nls --disable-dependency-tracking
sh ./build.sh

cp make "$OUT"
""".format(
        compiler = cc.compiler,
        out = make_bin.path,
        src_dir = src_dir,
        cflags = cc.cflags,
        ldflags = cc.ldflags,
        toolbin = TOOLBIN_SNIPPET,
    )

    ctx.actions.run_shell(
        outputs = [make_bin],
        inputs = depset(
            direct = ctx.files.srcs,
            transitive = [cc_toolchain.all_files],
        ),
        command = command,
        mnemonic = "BuildMake",
        progress_message = "Bootstrapping GNU make from source for %s" % ctx.label,
    )

    return [DefaultInfo(files = depset([make_bin]))]

build_make = rule(
    implementation = _build_make_impl,
    attrs = {
        "srcs": attr.label(
            mandatory = True,
            doc = "The GNU make source tree (filegroup).",
        ),
        "configure": attr.label(
            mandatory = True,
            allow_single_file = True,
            doc = "The `configure` script at the root of the source tree.",
        ),
        "make_binary": attr.output(
            mandatory = True,
            doc = "Output path for the built make binary (e.g. bin/make).",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
