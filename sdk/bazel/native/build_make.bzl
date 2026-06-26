load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

_PATH_PREFIXES = ["-L", "-B", "-I", "-F", "-iquote"]

def _rel(p):
    return p.startswith("external/") or p.startswith("bazel-out/")

def _abs(tok):
    if _rel(tok):
        return "$EXECROOT/" + tok
    for p in _PATH_PREFIXES:
        if tok.startswith(p) and _rel(tok[len(p):]):
            return p + "$EXECROOT/" + tok[len(p):]
    return tok

# Per-compilation flags configure/build.sh add themselves.
_DROP = ["-c", "-S", "-E", "-o"]

def _flags(fc, action_name, variables):
    out = []
    for f in cc_common.get_memory_inefficient_command_line(
        feature_configuration = fc,
        action_name = action_name,
        variables = variables,
    ):
        if f in _DROP:
            continue
        out.append(_abs(f))
    return out

def _build_make_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    fc = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    compiler = cc_common.get_tool_for_action(
        feature_configuration = fc,
        action_name = ACTION_NAMES.c_compile,
    )

    # Bare clang can't link an executable without the toolchain's sysroot/CRT
    # flags; pass the real compile/link command lines as CFLAGS/LDFLAGS.
    compile_vars = cc_common.create_compile_variables(
        feature_configuration = fc,
        cc_toolchain = cc_toolchain,
    )
    link_vars = cc_common.create_link_variables(
        feature_configuration = fc,
        cc_toolchain = cc_toolchain,
        is_linking_dynamic_library = False,
    )
    cflags = " ".join(_flags(fc, ACTION_NAMES.c_compile, compile_vars))
    ldflags = " ".join(_flags(fc, ACTION_NAMES.cpp_link_executable, link_vars))

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

# configure/build.sh probe $PATH for binutils (ld/ar/ranlib/nm); the sandbox
# has none, so expose the toolchain's lld/llvm-* under their plain names.
CLANGDIR="$(dirname "$CLANG")"
TOOLBIN="$(mktemp -d)"
ln -s "$CLANGDIR/ld.lld" "$TOOLBIN/ld"
ln -s "$CLANGDIR/llvm-ar" "$TOOLBIN/ar"
ln -s "$CLANGDIR/llvm-ranlib" "$TOOLBIN/ranlib"
ln -s "$CLANGDIR/llvm-nm" "$TOOLBIN/nm"
ln -s "$CLANGDIR/llvm-strip" "$TOOLBIN/strip"
export PATH="$TOOLBIN:$PATH"

cd "$SRC"

# build.sh compiles make without needing make itself.
./configure --disable-nls --disable-dependency-tracking
sh ./build.sh

cp make "$OUT"
""".format(
        compiler = compiler,
        out = make_bin.path,
        src_dir = src_dir,
        cflags = cflags,
        ldflags = ldflags,
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
