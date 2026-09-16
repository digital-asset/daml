load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("//bazel/native:hermetic_cc.bzl", "JOBS_SNIPPET", "TOOLBIN_SNIPPET", "hermetic_cc_flags")
load("@os_info//:os_info.bzl", "is_windows")

InstalledGnuToolInfo = provider(
    doc = "Paths within a GNU tool install prefix.",
    fields = {
        "prefix": "The directory tree artifact containing the installed tool.",
        "bindir": "Relative path to bin/ within prefix.",
        "datadir": "Relative path to share/ within prefix.",
    },
)

_EXECROOT_LINES = [
    'EXECROOT="$PWD"',
    'EXECROOT_NATIVE="$(pwd -W)"' if is_windows else 'EXECROOT_NATIVE="$PWD"',
]

_PERL5SHELL_LINE = 'export PERL5SHELL="sh -c"' if is_windows else ""

_HOST_TRIPLE_FLAGS = [
    "--build=x86_64-w64-mingw32",
    "--host=x86_64-w64-mingw32",
] if is_windows else []

def executable_by_name(files, name):
    for f in files:
        if f.basename == name:
            return f
    for f in files:
        if f.basename == name + ".exe":
            return f
    return None

def _find_pre_build_configure(dep_src):
    dep_files = dep_src.files.to_list()
    for f in dep_files:
        if f.basename == "configure" and f.dirname.count("/") == dep_files[0].dirname.count("/"):
            return f
    return None

def _build_gnu_tool_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    cc = hermetic_cc_flags(ctx, cc_toolchain)

    configure_src = ctx.file.configure
    make_bin = executable_by_name(ctx.files.make, "make")
    if not make_bin:
        fail("no make executable in {}".format(ctx.attr.make.label))
    configure_flags = " ".join(_HOST_TRIPLE_FLAGS + ctx.attr.configure_flags)

    # Optional perl binary (perl-based tools such as autoconf/automake).
    perl_bin = None
    if ctx.files.perl:
        perl_bin = executable_by_name(ctx.files.perl, "perl")
        if not perl_bin:
            fail("no perl executable in {}".format(ctx.attr.perl.label))

    # Extra tool binaries placed on PATH during the build (e.g. m4).
    extra_path_entries = ['"$EXECROOT/{}"'.format(f.dirname) for f in ctx.files.extra_tools]
    m4_env_line = ""
    m4_bin = executable_by_name(ctx.files.extra_tools, "m4")
    if m4_bin:
        m4_env_line = 'export M4="$EXECROOT/{}"'.format(m4_bin.path)

    # --- Common environment preamble ---
    env_lines = [
        "set -euo pipefail",
    ] + _EXECROOT_LINES + [
        'CLANG="$EXECROOT/{}"'.format(cc.compiler),
        # lld ships next to clang (sandbox has no host ld); CFLAGS/CPPFLAGS/LDFLAGS
        # carry the sysroot so configure's compile/preprocess probes stay hermetic.
        'export CC="$CLANG -fuse-ld=lld"',
        'export CFLAGS="{}"'.format(cc.cflags),
        'export CPPFLAGS="{}"'.format(cc.cflags),
        'export LDFLAGS="{}"'.format(cc.ldflags),
        TOOLBIN_SNIPPET,
        JOBS_SNIPPET,
        'MAKE="$EXECROOT/{}"'.format(make_bin.path),
        'SRC="$EXECROOT/$(dirname {})"'.format(configure_src.path),
        _PERL5SHELL_LINE,
        'TMP="$(mktemp -d)"',
        'BUILD="$TMP/build"',
        'cp -rpL "$SRC/." "$BUILD"',
        'chmod -R u+w "$BUILD"',
    ]
    if perl_bin:
        env_lines.append(
            'PERL="$(command -v perl)"' if is_windows else 'PERL="$EXECROOT/{}"'.format(perl_bin.path),
        )

    inputs = ctx.files.srcs + ctx.files.extra_tools + ctx.files.perl + ctx.files.make

    if ctx.attr.built_path:
        # --- Build mode: compile a single binary, no install/prefix. ---
        if not ctx.outputs.binary:
            fail("`binary` output must be set when `built_path` is provided.")
        out_binary = ctx.outputs.binary
        path_entries = ['"$(dirname "$MAKE")"'] + extra_path_entries
        body = [
            'export PATH={}:"$PATH"'.format(":".join(path_entries)),
            m4_env_line,
            'cd "$BUILD"',
            "./configure {}".format(configure_flags),
            '"$MAKE" -j {}'.format(" ".join([
                '"{}"'.format(a)
                for a in ctx.attr.make_args
            ])),
            'if [ -f "{built}" ]; then BUILT="{built}"; else BUILT="{built}.exe"; fi'.format(
                built = ctx.attr.built_path,
            ),
            'cat "$BUILT" > "$EXECROOT/{out}"'.format(out = out_binary.path),
            'chmod +x "$EXECROOT/{out}"'.format(out = out_binary.path),
            'rm -rf "$TMP"',
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
                'DEP_BUILD="$TMP/pre/{}"'.format(dep_configure.dirname.replace("/", "_")),
                'mkdir -p "$DEP_BUILD"',
                'cp -rpL "$EXECROOT/{}/." "$DEP_BUILD"'.format(dep_configure.dirname),
                'chmod -R u+w "$DEP_BUILD"',
                'cd "$DEP_BUILD"',
                '{}./configure --prefix="$PREFIX_NATIVE"'.format('PERL="$PERL" ' if perl_bin else ""),
                '"$MAKE" -j"$JOBS" install',
                'cd "$EXECROOT"',
            ]))

    path_entries = ['"$PREFIX/bin"', '"$(dirname "$MAKE")"'] + extra_path_entries
    if perl_bin:
        path_entries.append('"$(dirname "$PERL")"')

    body = [
        'PREFIX="$EXECROOT/{}"'.format(prefix_dir.path),
        'PREFIX_NATIVE="$EXECROOT_NATIVE/{}"'.format(prefix_dir.path),
        'export PATH={}:"$PATH"'.format(":".join(path_entries)),
        m4_env_line,
        "",
        "\n".join(pre_build_steps),
        "",
        'echo "=== Building main package ==="',
        'cd "$BUILD"',
        '{}./configure --prefix="$PREFIX_NATIVE" {}'.format(
            'PERL="$PERL" ' if perl_bin else "",
            configure_flags,
        ),
        '"$MAKE" -j"$JOBS" install',
        "",
        "# Fix shebangs to be relocatable (use env perl instead of hardcoded path).",
        'for f in "$PREFIX/bin/"*; do',
        '    [ -f "$f" ] || continue',
        '    if head -1 "$f" | grep -q perl; then',
        "        sed -i.bak '1s|#!.*perl.*|#!/usr/bin/env perl|' \"$f\" && rm -f \"$f.bak\"",
        "    fi",
        "done",
        "",
        "# Replace hardcoded sandbox-absolute paths with a relocatable placeholder so",
        "# consumers can restore them with a single sed 's|__EXECROOT__|'\"$PWD\"'|g'.",
        'FILELIST="$(mktemp)"',
        'find "$PREFIX" -type f > "$FILELIST"',
        "while IFS= read -r pf; do",
        '    if grep -Iq . "$pf"; then',
        '        sed -i.bak -e "s|$EXECROOT_NATIVE|__EXECROOT__|g" -e "s|$EXECROOT|__EXECROOT__|g" "$pf" && rm -f "$pf.bak"',
        "    fi",
        'done < "$FILELIST"',
        'rm -f "$FILELIST"',
        'rm -rf "$TMP"',
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
            allow_files = True,
            doc = "The hermetic `make` binary, plus any runtime libraries beside it.",
        ),
        "make_args": attr.string_list(
            doc = "Extra arguments for the build-mode `make` invocation, e.g. a SUBDIRS override.",
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
