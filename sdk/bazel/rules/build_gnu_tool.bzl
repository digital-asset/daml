load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

def _build_gnu_tool_impl(ctx):
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

    configure = ctx.file.configure
    src_dir = configure.dirname
    make_bin = ctx.file.make
    out_binary = ctx.outputs.binary

    configure_flags = " ".join(ctx.attr.configure_flags)

    command = """\
set -euo pipefail

EXECROOT="$PWD"
CC="$EXECROOT/{compiler}"
MAKE="$EXECROOT/{make}"
OUT="$EXECROOT/{out}"
SRC="$EXECROOT/{src_dir}"

cd "$SRC"

# lld ships next to clang; the sandbox has no host ld.
LDFLAGS="-fuse-ld=lld"

CC="$CC" LDFLAGS="$LDFLAGS" ./configure {configure_flags}
CC="$CC" LDFLAGS="$LDFLAGS" "$MAKE" -j

cp "{built_path}" "$OUT"
""".format(
        compiler = compiler,
        make = make_bin.path,
        out = out_binary.path,
        src_dir = src_dir,
        configure_flags = configure_flags,
        built_path = ctx.attr.built_path,
    )

    ctx.actions.run_shell(
        outputs = [out_binary],
        inputs = depset(
            direct = ctx.files.srcs + [make_bin],
            transitive = [cc_toolchain.all_files],
        ),
        command = command,
        mnemonic = "BuildGnuTool",
        progress_message = "Building %s from source for %s" % (ctx.attr.built_path, ctx.label),
    )

    return [DefaultInfo(files = depset([out_binary]))]

build_gnu_tool = rule(
    implementation = _build_gnu_tool_impl,
    attrs = {
        "srcs": attr.label(
            mandatory = True,
            doc = "The GNU source tree (filegroup).",
        ),
        "configure": attr.label(
            mandatory = True,
            allow_single_file = True,
            doc = "The `configure` script at the root of the source tree.",
        ),
        "make": attr.label(
            mandatory = True,
            allow_single_file = True,
            doc = "The hermetic `make` binary used to drive the build.",
        ),
        "built_path": attr.string(
            mandatory = True,
            doc = "Path of the built binary within the source tree (e.g. src/m4).",
        ),
        "binary": attr.output(
            mandatory = True,
            doc = "Output path for the built binary.",
        ),
        "configure_flags": attr.string_list(
            default = ["--disable-nls", "--disable-dependency-tracking"],
            doc = "Flags passed to ./configure.",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
