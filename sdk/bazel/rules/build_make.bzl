load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

def _build_make_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    compiler = cc_common.get_tool_for_action(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.c_compile,
    )

    make_bin = ctx.outputs.make_binary
    configure = ctx.file.configure
    src_dir = configure.dirname

    command = """\
set -euo pipefail

EXECROOT="$PWD"
CC="$EXECROOT/{compiler}"
OUT="$EXECROOT/{out}"
SRC="$EXECROOT/{src_dir}"

cd "$SRC"

# lld ships next to clang; the sandbox has no host ld.
LDFLAGS="-fuse-ld=lld"

# build.sh compiles make without needing make itself.
CC="$CC" LDFLAGS="$LDFLAGS" ./configure --disable-nls --disable-dependency-tracking
CC="$CC" LDFLAGS="$LDFLAGS" sh ./build.sh

cp make "$OUT"
""".format(
        compiler = compiler,
        out = make_bin.path,
        src_dir = src_dir,
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
