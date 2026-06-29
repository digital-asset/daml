load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("//bazel/native:hermetic_cc.bzl", "TOOLBIN_SNIPPET", "hermetic_cc_flags")

# Runs ./configure && make && make install at action time on the hermetic cc
# toolchain. CC/CFLAGS/CPPFLAGS/LDFLAGS (+ a binutils tool dir) carry the
# toolchain's sysroot so configure builds and probes against it, not host libc.

def _configure_make_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    cc = hermetic_cc_flags(ctx, cc_toolchain, link_dynamic = True)
    cc_exe = hermetic_cc_flags(ctx, cc_toolchain, link_dynamic = False)

    extra_cflags = list(ctx.attr.extra_cflags)
    if ctx.file.version_script:
        # ncurses' MK_SHARED_LIB recipe uses ${CC} ${CFLAGS} but not ${LDFLAGS},
        # so the version script must ride in through CFLAGS to reach the link.
        extra_cflags.append("-Wl,--version-script=$EXECROOT/{}".format(ctx.file.version_script.path))
    cflags = " ".join([cc.cflags] + extra_cflags)

    configure = ctx.file.configure
    make_bin = ctx.file.make
    src_dir = configure.dirname
    flags = " ".join(ctx.attr.configure_flags)

    inputs = ctx.files.srcs + [make_bin]
    if ctx.file.version_script:
        inputs = inputs + [ctx.file.version_script]

    env_lines = [
        "set -euo pipefail",
        'EXECROOT="$PWD"',
        'CLANG="$EXECROOT/{}"'.format(cc.compiler),
        # lld ships next to clang; the sandbox has no host ld.
        'export CC="$CLANG -fuse-ld=lld"',
        'export AR="$EXECROOT/{}"'.format(cc.ar),
        'export CFLAGS="{}"'.format(cflags),
        # configure's preprocessor probes use $CPP/$CPPFLAGS; without the sysroot
        # they fall back to host /lib/cpp (absent in the sandbox).
        'export CPP="$CLANG -E"',
        'export CPPFLAGS="{}"'.format(cc.cflags),
        'export LDFLAGS="{}"'.format(cc.ldflags),
        # Build-time helper programs (e.g. ncurses' make_hash) are compiled with
        # the hermetic sysroot too -- the sandbox has no host headers/libc -- and
        # run on the system glibc at runtime (link-time stubs, not in RUNPATH).
        # lld stays in BUILD_LDFLAGS (not BUILD_CC) so $BUILD_CC is textually
        # distinct from $CC -- ncurses 5.9 errors if they are equal.
        'export BUILD_CC="$CLANG"',
        'export BUILD_CFLAGS="{}"'.format(cc.cflags),
        'export BUILD_LDFLAGS="-fuse-ld=lld {}"'.format(cc_exe.ldflags),
        TOOLBIN_SNIPPET,
        'MAKE="$EXECROOT/{}"'.format(make_bin.path),
        # configure runs bare `make` for its own probes; expose the hermetic one
        # (the sandbox has no host make). $TOOLBIN is already on PATH.
        'ln -s "$MAKE" "$TOOLBIN/make"',
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
        "extra_cflags": attr.string_list(doc = "Extra flags appended to CFLAGS (e.g. quieting legacy-source warnings)."),
        "version_script": attr.label(allow_single_file = True, doc = "Optional linker version script, injected via CFLAGS (see impl)."),
        "install_libdir": attr.string(default = "lib", doc = "Install-prefix subdir holding the declared outputs."),
        "outs": attr.output_list(mandatory = True, doc = "Installed artifacts to expose; copied from <prefix>/<install_libdir>/<basename>."),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
