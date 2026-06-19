"""GHC bindist `./configure && make install` as a Bazel action (not fetch), so
the C compiler is the registered hermetic LLVM cc toolchain at action time."""

load("@rules_cc//cc:action_names.bzl", "ACTION_NAMES")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")

# Launchers exec the real binary under <install>/lib/bin: the post-install
# `bin/` scripts hard-code the install prefix and are not relocatable across
# sandboxes, so `ghc`/`ghc-pkg` get an explicit `-B`/`--global-package-db`.
_LAUNCHER_EXTRA_ARGS = {
    "ghc": '-B"$ROOT/lib"',
    "ghc-pkg": '--global-package-db "$ROOT/lib/package.conf.d"',
}
_TOOLS = ["ghc", "ghc-pkg", "hsc2hs", "haddock", "runghc", "hpc"]

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

def _make_launcher(ctx, install_tree, tool_name):
    launcher = ctx.actions.declare_file("{}_bin/{}".format(ctx.label.name, tool_name))
    ctx.actions.write(
        output = launcher,
        is_executable = True,
        content = """#!/usr/bin/env bash
set -euo pipefail
ROOT="$PWD/{tree}"
exec "$ROOT/lib/bin/{tool}" {extra} "$@"
""".format(
            tree = install_tree.path,
            tool = tool_name,
            extra = _LAUNCHER_EXTRA_ARGS.get(tool_name, ""),
        ),
    )
    return launcher

def _ghc_bindist_install_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    compiler = _compiler_path(ctx, cc_toolchain)

    configure = ctx.file.configure
    install_tree = ctx.actions.declare_directory(ctx.label.name + "_install")

    # Sentinel so haskell_toolchain can infer a libdir_path; real libdir reaches
    # ghc via the launcher's `-B`.
    lib_settings = ctx.actions.declare_file("{}_layout/lib/settings".format(ctx.label.name))
    doc_marker = ctx.actions.declare_file(ctx.label.name + "_doc_marker")

    launchers = [_make_launcher(ctx, install_tree, tool) for tool in _TOOLS]

    command = """\
set -euo pipefail

EXECROOT="$PWD"
SRC="$EXECROOT/$(dirname {configure})"
PREFIX="$EXECROOT/{prefix}"
CC_BIN="$EXECROOT/{compiler}"
MAKE_BIN="$EXECROOT/{make}"

TMP="$(mktemp -d)"
BUILD="$TMP/build"

cp -rpL "$SRC/." "$BUILD"
chmod -R u+w "$BUILD"
cd "$BUILD"

# Must enable RelocatableBuild before configure/make so the generated package
# db uses ${{pkgroot}}/$topdir-relative paths.
sed -e "s/RelocatableBuild = NO/RelocatableBuild = YES/" -i.bak mk/config.mk.in
rm -f mk/config.mk.in.bak

# Hermetic clang + make dirs first; /usr/bin:/bin only for host coreutils/bash.
export PATH="$(dirname "$CC_BIN"):$(dirname "$MAKE_BIN"):/usr/bin:/bin:$PATH"
export CC="$CC_BIN -fuse-ld=lld"

./configure --prefix "$PREFIX"
"$MAKE_BIN" -j"$(nproc)" install

# Bundle hermetic libgmp.so into integer-gmp's libdir so its `-lgmp` resolves
# on every Haskell link; the deb9 bindist otherwise expects a host libgmp.
for f in {gmp_libs}; do
    cp -L "$EXECROOT/$f" "$PREFIX/lib/integer-gmp-1.1/"
done

cp "$PREFIX/lib/settings" "$EXECROOT/{lib_settings}"
if [ -d "$PREFIX/doc" ]; then
    touch "$EXECROOT/{doc_marker}"
else
    echo "no-docs" > "$EXECROOT/{doc_marker}"
fi

rm -rf "$TMP"
""".format(
        configure = configure.path,
        prefix = install_tree.path,
        compiler = compiler,
        make = ctx.file.make.path,
        lib_settings = lib_settings.path,
        doc_marker = doc_marker.path,
        gmp_libs = " ".join([f.path for f in ctx.files.gmp]),
    )

    ctx.actions.run_shell(
        outputs = [install_tree, lib_settings, doc_marker],
        inputs = depset(
            direct = ctx.files.srcs + [configure, ctx.file.make] + ctx.files.gmp,
            transitive = [cc_toolchain.all_files],
        ),
        command = command,
        mnemonic = "GhcBindistInstall",
        progress_message = "Installing GHC bindist (configure && make install) for {}".format(ctx.label),
        use_default_shell_env = False,
    )

    return [
        DefaultInfo(
            files = depset([install_tree, lib_settings, doc_marker] + launchers),
            # cabal-mode packages run Setup.hs via rules_haskell's runghc/
            # cabal_wrapper, which resolve GHC through runfiles; propagate the
            # install tree + launchers so those tools find lib/bin/ghc.
            runfiles = ctx.runfiles(files = [install_tree, lib_settings, doc_marker] + launchers),
        ),
        # Single-file handle on the tree dir so the lock generator can take it
        # as one input ($(rlocationpath) needs an unambiguous output).
        OutputGroupInfo(install_tree = depset([install_tree])),
    ]

ghc_bindist_install = rule(
    implementation = _ghc_bindist_install_impl,
    attrs = {
        "srcs": attr.label(
            mandatory = True,
            doc = "Raw bindist filegroup from @ghc_bindist.",
        ),
        "configure": attr.label(
            mandatory = True,
            allow_single_file = True,
            doc = "bindist_unpacked/configure file from @ghc_bindist.",
        ),
        "make": attr.label(
            mandatory = True,
            allow_single_file = True,
            doc = "Hermetic make binary (built as an action), e.g. @make//:make.",
        ),
        "gmp": attr.label(
            allow_files = True,
            doc = "Hermetic libgmp.so(s) bundled into integer-gmp's libdir, e.g. @gmp//:libs.",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
