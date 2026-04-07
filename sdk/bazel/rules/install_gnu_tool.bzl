InstalledGnuToolInfo = provider(
    doc = "Paths within a GNU tool install prefix.",
    fields = {
        "prefix": "The directory tree artifact containing the installed tool.",
        "bindir": "Relative path to bin/ within prefix.",
        "datadir": "Relative path to share/ within prefix.",
    },
)

def _install_gnu_tool_impl(ctx):
    prefix_dir = ctx.actions.declare_directory(ctx.attr.name + "_prefix")

    configure_src = ctx.file.configure
    perl_files = ctx.files.perl
    perl_bin = [f for f in perl_files if f.basename == "perl"][0]

    extra_path_entries = [
        "$EXECROOT/" + f.dirname
        for f in ctx.files.extra_tools
    ]

    m4_env_line = ""
    for f in ctx.files.extra_tools:
        if f.basename == "m4":
            m4_env_line = 'export M4="$EXECROOT/{}"'.format(f.path)

    # Build additional source packages before the main one. Each entry
    # is built via ./configure && make install into the shared prefix
    # so that all tools share consistent internal paths.
    pre_build_steps = []
    for dep_src in ctx.attr.pre_build_srcs:
        dep_files = dep_src.files.to_list()
        dep_configure = None
        for f in dep_files:
            if f.basename == "configure" and f.dirname.count("/") == dep_files[0].dirname.count("/"):
                dep_configure = f
                break
        if dep_configure:
            pre_build_steps.append("""\
echo "=== Pre-building from {src_dir} ==="
cd "$EXECROOT/$(dirname {configure})"
PERL="$PERL" ./configure --prefix="$PREFIX"
make -j$(nproc) install
cd "$EXECROOT"
""".format(configure = dep_configure.path, src_dir = dep_configure.dirname))

    path_str = ":".join(extra_path_entries)

    all_srcs = ctx.files.srcs + ctx.files.extra_tools + perl_files
    for dep_src in ctx.attr.pre_build_srcs:
        all_srcs = all_srcs + dep_src.files.to_list()

    inputs = depset(direct = all_srcs)

    ctx.actions.run_shell(
        outputs = [prefix_dir],
        inputs = inputs,
        command = """\
set -euo pipefail
EXECROOT="$PWD"
PERL="$EXECROOT/{perl}"
PREFIX="$EXECROOT/{output}"
SRC="$EXECROOT/$(dirname {configure})"
EXTRA_PATH="{extra_path}"

export PATH="$PREFIX/bin:$(dirname $PERL):$EXTRA_PATH:$PATH"
{m4_env}

{pre_build_steps}

echo "=== Building main package ==="
cd "$SRC"
PERL="$PERL" ./configure --prefix="$PREFIX"
make -j$(nproc) install

# Fix shebangs to be relocatable (use env perl instead of hardcoded path).
for f in "$PREFIX/bin/"*; do
    [ -f "$f" ] || continue
    if head -1 "$f" | grep -q perl; then
        sed -i '1s|#!.*perl.*|#!/usr/bin/env perl|' "$f"
    fi
done

# Replace all hardcoded sandbox-absolute paths with a relocatable placeholder.
# Autotools bakes the absolute install prefix, perl path, and m4 path into
# scripts and config files.  Replacing the common execroot prefix lets the
# consumer do a single: sed 's|__EXECROOT__|'"$PWD"'|g' to fix everything,
# since execroot-relative artifact paths are stable across sandboxes.
find "$PREFIX" -type f | while IFS= read -r pf; do
    if file --mime-type "$pf" | grep -q text; then
        sed -i "s|$EXECROOT|__EXECROOT__|g" "$pf"
    fi
done
""".format(
            configure = configure_src.path,
            perl = perl_bin.path,
            output = prefix_dir.path,
            extra_path = path_str,
            m4_env = m4_env_line,
            pre_build_steps = "\n".join(pre_build_steps),
        ),
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

install_gnu_tool = rule(
    implementation = _install_gnu_tool_impl,
    attrs = {
        "srcs": attr.label(mandatory = True, doc = "Source tree filegroup."),
        "configure": attr.label(
            mandatory = True,
            allow_single_file = True,
            doc = "The configure script for the main package.",
        ),
        "perl": attr.label(mandatory = True, doc = "Perl toolchain files."),
        "extra_tools": attr.label_list(
            allow_files = True,
            doc = "Additional tool binaries needed on PATH during build (individual files).",
        ),
        "pre_build_srcs": attr.label_list(
            doc = "Source trees to build and install before the main package. " +
                  "Each must contain a configure script at its root.",
        ),
    },
)
