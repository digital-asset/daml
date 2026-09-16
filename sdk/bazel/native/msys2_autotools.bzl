load("//bazel/native:build_gnu_tool.bzl", "InstalledGnuToolInfo")

_UNVERSIONED = [
    ("autoconf", "autoconf"),
    ("autoheader", "autoconf"),
    ("autom4te", "autoconf"),
    ("autoreconf", "autoconf"),
    ("autoscan", "autoconf"),
    ("autoupdate", "autoconf"),
    ("ifnames", "autoconf"),
    ("aclocal", "automake"),
    ("automake", "automake"),
]

def _package_roots(files):
    roots = {}
    for f in files:
        if "/usr/" in f.path:
            roots[f.path[:f.path.index("/usr/")]] = None
    return sorted(roots)

def _msys2_autotools_impl(ctx):
    prefix = ctx.actions.declare_directory(ctx.attr.name + "_prefix")
    placeholder = "__EXECROOT__/" + prefix.path

    inputs = ctx.files.m4 + ctx.files.autoconf + ctx.files.automake
    roots = _package_roots(inputs)
    if len(roots) != 3:
        fail("expected three MSYS2 package roots (m4, autoconf, automake), got {}".format(roots))

    versions = {"autoconf": ctx.attr.autoconf_version, "automake": ctx.attr.automake_series}

    sed_exprs = " ".join([
        '-e "s|{}|{}|g"'.format(baked, replacement)
        for baked, replacement in [
            ("/usr/bin/m4", placeholder + "/bin/m4.exe"),
            ("/usr/bin/auto", placeholder + "/bin/auto"),
            ("/usr/share/autoconf-", placeholder + "/share/autoconf-"),
            ("/usr/share/automake-", placeholder + "/share/automake-"),
            ("/usr/share/aclocal", placeholder + "/share/aclocal"),
        ]
    ])

    command = "\n".join([
        "set -euo pipefail",
        'PREFIX="$PWD/{}"'.format(prefix.path),
        'mkdir -p "$PREFIX"',
    ] + [
        'cp -rL "{}/usr/." "$PREFIX/"'.format(root)
        for root in roots
    ] + [
        'chmod -R u+w "$PREFIX"',
        "",
        "# autoreconf looks its helpers up on PATH under unversioned names.",
    ] + [
        'cp -p "$PREFIX/bin/{n}-{v}" "$PREFIX/bin/{n}"'.format(n = name, v = versions[tool])
        for name, tool in _UNVERSIONED
    ] + [
        "",
        'RELOCATE="$(mktemp)"',
        'find "$PREFIX/bin" -type f > "$RELOCATE"',
        'find "$PREFIX/share" -name autom4te.cfg -o -name Config.pm >> "$RELOCATE"',
        "while IFS= read -r f; do",
        '    grep -Iq . "$f" || continue',
        '    sed -i.bak {} "$f" && rm -f "$f.bak"'.format(sed_exprs),
        'done < "$RELOCATE"',
        'rm -f "$RELOCATE"',
    ])

    ctx.actions.run_shell(
        outputs = [prefix],
        inputs = depset(inputs),
        command = command,
        mnemonic = "AssembleMsys2Autotools",
        progress_message = "Assembling MSYS2 autotools for %s" % ctx.label,
        use_default_shell_env = False,
    )

    return [
        DefaultInfo(files = depset([prefix])),
        InstalledGnuToolInfo(prefix = prefix, bindir = "bin", datadir = "share"),
    ]

msys2_autotools = rule(
    implementation = _msys2_autotools_impl,
    attrs = {
        "m4": attr.label(mandatory = True, allow_files = True),
        "autoconf": attr.label(mandatory = True, allow_files = True),
        "automake": attr.label(mandatory = True, allow_files = True),
        "autoconf_version": attr.string(mandatory = True),
        "automake_series": attr.string(mandatory = True),
    },
)
