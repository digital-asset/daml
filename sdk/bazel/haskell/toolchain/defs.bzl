GhcInfo = provider(
    fields = ["ghc", "ghc_pkg", "version", "all_files"],
)

def _by_name(files, name):
    for f in files:
        if f.basename == name:
            return f
    fail("tool '{}' not found in install".format(name))

def _ghc_toolchain_impl(ctx):
    files = ctx.files.install
    return [platform_common.ToolchainInfo(
        ghc = GhcInfo(
            ghc = _by_name(files, "ghc"),
            ghc_pkg = _by_name(files, "ghc-pkg"),
            version = ctx.attr.version,
            all_files = depset(files),
        ),
    )]

ghc_toolchain = rule(
    implementation = _ghc_toolchain_impl,
    attrs = {
        "install": attr.label(mandatory = True),
        "version": attr.string(mandatory = True),
    },
)

_TYPE = "//bazel/haskell/toolchain:ghc_toolchain_type"

def _ghc_smoke_impl(ctx):
    ghc = ctx.toolchains[_TYPE].ghc
    out = ctx.actions.declare_file(ctx.label.name + ".version")
    ctx.actions.run_shell(
        outputs = [out],
        inputs = ghc.all_files,
        command = '"{ghc}" --version > "{out}"'.format(ghc = ghc.ghc.path, out = out.path),
        mnemonic = "GhcSmoke",
        use_default_shell_env = False,
    )
    return [DefaultInfo(files = depset([out]))]

ghc_smoke = rule(
    implementation = _ghc_smoke_impl,
    toolchains = [_TYPE],
)
