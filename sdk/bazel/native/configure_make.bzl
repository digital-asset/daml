load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

def _tool_for(ctx, cc_toolchain, action_name):
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    return cc_common.get_tool_for_action(
        feature_configuration = feature_configuration,
        action_name = action_name,
    )

def _configure_make_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    cc = _tool_for(ctx, cc_toolchain, ACTION_NAMES.c_compile)
    ar = _tool_for(ctx, cc_toolchain, ACTION_NAMES.cpp_link_static_library)

    configure = ctx.file.configure
    make_bin = ctx.file.make
    src_dir = configure.dirname
    flags = " ".join(ctx.attr.configure_flags)

    inputs = ctx.files.srcs + [make_bin]

    env_lines = [
        "set -euo pipefail",
        'EXECROOT="$PWD"',
        # Bake `-fuse-ld=lld` into CC: lld ships next to clang and the sandbox
        # has no host `ld`. configure probes that link but ignore LDFLAGS (e.g.
        # gmp's) otherwise fail trying to spawn a non-existent default linker.
        'export CC="$EXECROOT/{} -fuse-ld=lld"'.format(cc),
        'export AR="$EXECROOT/{}"'.format(ar),
        'MAKE="$EXECROOT/{}"'.format(make_bin.path),
        'SRC="$EXECROOT/{}"'.format(src_dir),
    ]
    if ctx.file.m4:
        inputs = inputs + [ctx.file.m4]
        env_lines.append('export M4="$EXECROOT/{}"'.format(ctx.file.m4.path))

    # Build out-of-tree in a writable copy; install into a temp prefix, then
    # copy the declared outputs out, dereferencing the versioned-soname
    # symlinks so each output is a real file usable by `cc_library`.
    copy_lines = [
        'cp -L "$PREFIX/{libdir}/{base}" "$EXECROOT/{out}"'.format(
            libdir = ctx.attr.install_libdir,
            base = out.basename,
            out = out.path,
        )
        for out in ctx.outputs.outs
    ]

    body = [
        'TMP="$(mktemp -d)"',
        'BUILD="$TMP/build"',
        'PREFIX="$TMP/prefix"',
        'cp -rpL "$SRC/." "$BUILD"',
        'chmod -R u+w "$BUILD"',
        'cd "$BUILD"',
        './configure --prefix="$PREFIX" {}'.format(flags),
        '"$MAKE" -j$(nproc)',
        '"$MAKE" install',
    ] + copy_lines + ['rm -rf "$TMP"']

    ctx.actions.run_shell(
        outputs = ctx.outputs.outs,
        inputs = depset(direct = inputs, transitive = [cc_toolchain.all_files]),
        command = "\n".join(env_lines + body),
        mnemonic = "ConfigureMake",
        progress_message = "Building %s from source" % ctx.label,
        use_default_shell_env = False,
    )

    return [DefaultInfo(files = depset(ctx.outputs.outs))]

configure_make = rule(
    implementation = _configure_make_impl,
    attrs = {
        "srcs": attr.label(
            mandatory = True,
            doc = "The source tree (filegroup).",
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
        "m4": attr.label(
            allow_single_file = True,
            doc = "Optional hermetic `m4` binary exported as $M4 (e.g. for gmp asm).",
        ),
        "configure_flags": attr.string_list(
            doc = "Flags passed to ./configure (in addition to --prefix).",
        ),
        "install_libdir": attr.string(
            default = "lib",
            doc = "Subdirectory of the install prefix holding the declared outputs.",
        ),
        "outs": attr.output_list(
            mandatory = True,
            doc = "Installed artifacts to expose (e.g. lib/libgmp.so). Each is copied " +
                  "from <prefix>/<install_libdir>/<basename>, dereferencing symlinks.",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
