load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

# Runs ./configure && make && make install at action time on the hermetic cc
# toolchain. The toolchain's real compile/link flags (the glibc-2.28 -isystem
# set + --sysroot=/dev/null) are propagated into CFLAGS/LDFLAGS so configure
# builds against the sysroot, not host libc.

def _feature_config(ctx, cc_toolchain):
    return cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )

def _tool_for(fc, action_name):
    return cc_common.get_tool_for_action(feature_configuration = fc, action_name = action_name)

# Toolchain flag paths are execroot-relative (external/..., bazel-out/...).
# configure builds in a temp dir, so rewrite those to absolute $EXECROOT/...
# (shell expands $EXECROOT at action time). Handles both bare path tokens
# (`-isystem` + path) and combined prefix tokens (`-Lbazel-out/...`).
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

# Per-compilation flags configure/make add themselves.
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

def _configure_make_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    fc = _feature_config(ctx, cc_toolchain)
    cc = _tool_for(fc, ACTION_NAMES.c_compile)
    ar = _tool_for(fc, ACTION_NAMES.cpp_link_static_library)

    compile_vars = cc_common.create_compile_variables(
        feature_configuration = fc,
        cc_toolchain = cc_toolchain,
    )
    link_vars = cc_common.create_link_variables(
        feature_configuration = fc,
        cc_toolchain = cc_toolchain,
        is_linking_dynamic_library = True,
    )
    cflags = " ".join(_flags(fc, ACTION_NAMES.c_compile, compile_vars))
    ldflags = " ".join(_flags(fc, ACTION_NAMES.cpp_link_dynamic_library, link_vars))

    configure = ctx.file.configure
    make_bin = ctx.file.make
    src_dir = configure.dirname
    flags = " ".join(ctx.attr.configure_flags)

    inputs = ctx.files.srcs + [make_bin]

    env_lines = [
        "set -euo pipefail",
        'EXECROOT="$PWD"',
        # Bake `-fuse-ld=lld` into CC: lld ships next to clang and the sandbox
        # has no host `ld`.
        'export CC="$EXECROOT/{} -fuse-ld=lld"'.format(cc),
        'export AR="$EXECROOT/{}"'.format(ar),
        'export CFLAGS="{}"'.format(cflags),
        'export LDFLAGS="{}"'.format(ldflags),
        # Build-time helper programs must RUN in the sandbox, so compile them
        # with host libc (no hermetic sysroot flags); only the shipped target
        # CC output needs to be hermetic. Used by cross builds (--host!=--build).
        'export BUILD_CC="$EXECROOT/{} -fuse-ld=lld"'.format(cc),
        "export BUILD_CFLAGS=",
        "export BUILD_LDFLAGS=",
        'MAKE="$EXECROOT/{}"'.format(make_bin.path),
        'SRC="$EXECROOT/{}"'.format(src_dir),
    ]
    if ctx.file.m4:
        inputs = inputs + [ctx.file.m4]
        env_lines.append('export M4="$EXECROOT/{}"'.format(ctx.file.m4.path))

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
        "chmod +x ./configure",
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
        "srcs": attr.label(mandatory = True, doc = "The source tree (filegroup)."),
        "configure": attr.label(mandatory = True, allow_single_file = True, doc = "The `configure` script at the source root."),
        "make": attr.label(mandatory = True, allow_single_file = True, doc = "The hermetic `make` binary."),
        "m4": attr.label(allow_single_file = True, doc = "Optional hermetic `m4` exported as $M4."),
        "configure_flags": attr.string_list(doc = "Flags passed to ./configure (besides --prefix)."),
        "install_libdir": attr.string(default = "lib", doc = "Install-prefix subdir holding the declared outputs."),
        "outs": attr.output_list(mandatory = True, doc = "Installed artifacts to expose; copied from <prefix>/<install_libdir>/<basename>."),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
