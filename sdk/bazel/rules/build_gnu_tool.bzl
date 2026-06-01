load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

InstalledGnuToolInfo = provider(
    doc = "Paths within a GNU tool install prefix.",
    fields = {
        "prefix": "The directory tree artifact containing the installed tool.",
        "bindir": "Relative path to bin/ within prefix.",
        "datadir": "Relative path to share/ within prefix.",
    },
)

def _compiler_path(ctx, cc_toolchain):
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
        requested_features = ctx.features,
        unsupported_features = ctx.disabled_features,
    )
    return cc_common.get_tool_for_action(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.c_compile,
    )

def _find_pre_build_configure(dep_src):
    dep_files = dep_src.files.to_list()
    for f in dep_files:
        if f.basename == "configure" and f.dirname.count("/") == dep_files[0].dirname.count("/"):
            return f
    return None

def _build_gnu_tool_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    compiler = _compiler_path(ctx, cc_toolchain)

    configure_src = ctx.file.configure
    make_bin = ctx.file.make
    configure_flags = " ".join(ctx.attr.configure_flags)

    # Optional perl binary (perl-based tools such as autoconf/automake).
    perl_bin = None
    if ctx.files.perl:
        perl_bin = [f for f in ctx.files.perl if f.basename == "perl"][0]

    # Extra tool binaries placed on PATH during the build (e.g. m4).
    extra_path_entries = ['"$EXECROOT/{}"'.format(f.dirname) for f in ctx.files.extra_tools]
    m4_env_line = ""
    for f in ctx.files.extra_tools:
        if f.basename == "m4":
            m4_env_line = 'export M4="$EXECROOT/{}"'.format(f.path)

    # --- Common environment preamble ---
    env_lines = [
        "set -euo pipefail",
        'EXECROOT="$PWD"',
        'export CC="$EXECROOT/{}"'.format(compiler),
        'export LDFLAGS="-fuse-ld=lld"',  # lld ships next to clang; sandbox has no host ld.
        'MAKE="$EXECROOT/{}"'.format(make_bin.path),
        'SRC="$EXECROOT/$(dirname {})"'.format(configure_src.path),
    ]
    if perl_bin:
        env_lines.append('PERL="$EXECROOT/{}"'.format(perl_bin.path))

    inputs = ctx.files.srcs + ctx.files.extra_tools + ctx.files.perl + [make_bin]

    if ctx.attr.built_path:
        # --- Build mode: compile a single binary, no install/prefix. ---
        if not ctx.outputs.binary:
            fail("`binary` output must be set when `built_path` is provided.")
        out_binary = ctx.outputs.binary
        path_entries = ['"$(dirname "$MAKE")"'] + extra_path_entries
        body = [
            'export PATH={}:"$PATH"'.format(":".join(path_entries)),
            m4_env_line,
            'cd "$SRC"',
            './configure {}'.format(configure_flags),
            '"$MAKE" -j',
            'cp "{built}" "$EXECROOT/{out}"'.format(
                built = ctx.attr.built_path,
                out = out_binary.path,
            ),
        ]
        outputs = [out_binary]
        command = "\n".join(env_lines + body)
        ctx.actions.run_shell(
            outputs = outputs,
            inputs = depset(direct = inputs, transitive = [cc_toolchain.all_files]),
            command = command,
            mnemonic = "BuildGnuTool",
            progress_message = "Building %s from source for %s" % (ctx.attr.built_path, ctx.label),
            use_default_shell_env = False,
        )
        return [DefaultInfo(files = depset([out_binary]))]

    # --- Install mode: configure --prefix && make install into a shared prefix. ---
    prefix_dir = ctx.actions.declare_directory(ctx.attr.name + "_prefix")

    pre_build_steps = []
    for dep_src in ctx.attr.pre_build_srcs:
        dep_configure = _find_pre_build_configure(dep_src)
        if dep_configure:
            inputs = inputs + dep_src.files.to_list()
            pre_build_steps.append("\n".join([
                'echo "=== Pre-building from {} ==="'.format(dep_configure.dirname),
                'cd "$EXECROOT/$(dirname {})"'.format(dep_configure.path),
                '{}./configure --prefix="$PREFIX"'.format('PERL="$PERL" ' if perl_bin else ""),
                '"$MAKE" -j$(nproc) install',
                'cd "$EXECROOT"',
            ]))

    path_entries = ['"$PREFIX/bin"', '"$(dirname "$MAKE")"'] + extra_path_entries
    if perl_bin:
        path_entries.append('"$(dirname "$PERL")"')

    body = [
        'PREFIX="$EXECROOT/{}"'.format(prefix_dir.path),
        'export PATH={}:"$PATH"'.format(":".join(path_entries)),
        m4_env_line,
        "",
        "\n".join(pre_build_steps),
        "",
        'echo "=== Building main package ==="',
        'cd "$SRC"',
        '{}./configure --prefix="$PREFIX" {}'.format(
            'PERL="$PERL" ' if perl_bin else "",
            configure_flags,
        ),
        '"$MAKE" -j$(nproc) install',
        "",
        "# Fix shebangs to be relocatable (use env perl instead of hardcoded path).",
        'for f in "$PREFIX/bin/"*; do',
        '    [ -f "$f" ] || continue',
        '    if head -1 "$f" | grep -q perl; then',
        "        sed -i '1s|#!.*perl.*|#!/usr/bin/env perl|' \"$f\"",
        "    fi",
        "done",
        "",
        "# Replace hardcoded sandbox-absolute paths with a relocatable placeholder so",
        "# consumers can restore them with a single sed 's|__EXECROOT__|'\"$PWD\"'|g'.",
        'find "$PREFIX" -type f | while IFS= read -r pf; do',
        '    if file --mime-type "$pf" | grep -q text; then',
        '        sed -i "s|$EXECROOT|__EXECROOT__|g" "$pf"',
        "    fi",
        "done",
    ]
    command = "\n".join(env_lines + body)

    ctx.actions.run_shell(
        outputs = [prefix_dir],
        inputs = depset(direct = inputs, transitive = [cc_toolchain.all_files]),
        command = command,
        mnemonic = "InstallGnuTool",
        progress_message = "Building %s from source" % ctx.label,
        use_default_shell_env = False,
    )

    return [
        DefaultInfo(files = depset([prefix_dir])),
        InstalledGnuToolInfo(
            prefix = prefix_dir,
            bindir = "bin",
            datadir = "share",
        ),
    ]

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
        "configure_flags": attr.string_list(
            default = ["--disable-nls", "--disable-dependency-tracking"],
            doc = "Flags passed to ./configure.",
        ),
        "built_path": attr.string(
            default = "",
            doc = "Build mode: relative path of the built binary within the source " +
                  "tree (e.g. src/m4). When set, the tool is compiled (no install) " +
                  "and the binary is copied to `binary`. When empty, install mode is " +
                  "used (configure --prefix && make install into a prefix tree).",
        ),
        "binary": attr.output(
            doc = "Build-mode output path for the built binary (required iff built_path is set).",
        ),
        "perl": attr.label(
            doc = "Optional perl toolchain files (for perl-based tools).",
        ),
        "extra_tools": attr.label_list(
            allow_files = True,
            doc = "Additional tool binaries needed on PATH during the build (individual files).",
        ),
        "pre_build_srcs": attr.label_list(
            doc = "Install mode only: source trees built and installed into the shared " +
                  "prefix before the main package. Each must contain a root configure script.",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
