"""GHC bindist `./configure && make install` as a Bazel action (not fetch), so
the C compiler is the registered hermetic LLVM cc toolchain at action time."""

load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("//bazel/native:hermetic_cc.bzl", "TOOLBIN_SNIPPET", "hermetic_cc_flags")

# Launchers exec the real binary under <install>/lib/bin: the post-install
# `bin/` scripts hard-code the install prefix and are not relocatable across
# sandboxes, so `ghc`/`ghc-pkg` get an explicit `-B`/`--global-package-db`.
_LAUNCHER_EXTRA_ARGS = {
    "ghc": '-B"$ROOT/lib"',
    "ghc-pkg": '--global-package-db "$ROOT/lib/package.conf.d"',
    "runghc": '--ghc-arg=-B"$ROOT/lib"',
    "haddock": '-B"$ROOT/lib" -l"$ROOT/lib"',
    "hsc2hs": '--template="$ROOT/lib/template-hsc.h"',
}

# hsc2hs's bundled include must follow the caller's args, matching the stock wrapper.
_LAUNCHER_SUFFIX_ARGS = {
    "hsc2hs": '-I"$ROOT/lib/include/"',
}
_TOOLS = ["ghc", "ghc-pkg", "hsc2hs", "haddock", "runghc", "hpc"]

def _make_launcher(ctx, install_tree, tool_name):
    launcher = ctx.actions.declare_file("{}_bin/{}".format(ctx.label.name, tool_name))
    ctx.actions.write(
        output = launcher,
        is_executable = True,
        content = """#!/usr/bin/env bash
set -euo pipefail
SELF="$(readlink -f "${{BASH_SOURCE[0]}}")"
ROOT="$(cd "$(dirname "$SELF")/../{tree}" && pwd)"
exec "$ROOT/lib/bin/{tool}" {extra} "$@" {suffix}
""".format(
            tree = install_tree.basename,
            tool = tool_name,
            extra = _LAUNCHER_EXTRA_ARGS.get(tool_name, ""),
            suffix = _LAUNCHER_SUFFIX_ARGS.get(tool_name, ""),
        ),
    )
    return launcher

def _ghc_bindist_install_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    cc = hermetic_cc_flags(ctx, cc_toolchain)

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
CLANG="$EXECROOT/{compiler}"
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

export PATH="$(dirname "$CLANG"):$(dirname "$MAKE_BIN"):/usr/bin:/bin:$PATH"
export CC="$CLANG -fuse-ld=lld {cflags}"
export CFLAGS="{cflags}"
export CPPFLAGS="{cflags}"
export CPP="$CLANG -E {cflags}"
export LDFLAGS="{ldflags}"
{toolbin}

./configure --prefix "$PREFIX"
"$MAKE_BIN" -j"$(nproc)" install

# Bundle hermetic libgmp.so into the rts libdir, which is always on the link
# search path (the deb9 bindist otherwise expects a host libgmp); this lets
# every Haskell target resolve `-lgmp` with no per-target dependency.
for f in {gmp_libs}; do
    cp -L "$EXECROOT/$f" "$PREFIX/lib/rts/"
done

for f in {libz_libs}; do
    cp -L "$EXECROOT/$f" "$PREFIX/lib/rts/"
done

for f in {bz2_libs}; do
    cp -L "$EXECROOT/$f" "$PREFIX/lib/rts/"
done

# The deb9 ghc/ghc-pkg need libtinfo.so.5 at runtime (resolved via their
# RUNPATH $ORIGIN/../rts) and haskeline links -ltinfo (needs libtinfo.so);
# bundle our hermetic copy under both names so neither leaks the host's.
cp -L "$EXECROOT/{tinfo}" "$PREFIX/lib/rts/libtinfo.so"
cp -L "$EXECROOT/{tinfo}" "$PREFIX/lib/rts/libtinfo.so.5"

# configure bakes the absolute build-sandbox clang path into settings, which is
# non-reproducible; the consuming rules override the compiler via -pgm*, so
# replace it with a stable bare name to keep the install output cacheable.
sed -i \
    -e 's#("C compiler command", "[^"]*")#("C compiler command", "cc")#' \
    -e 's#("Haskell CPP command", "[^"]*")#("Haskell CPP command", "cc")#' \
    "$PREFIX/lib/settings"

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
        compiler = cc.compiler,
        cflags = cc.cflags,
        ldflags = cc.ldflags,
        toolbin = TOOLBIN_SNIPPET,
        make = ctx.file.make.path,
        lib_settings = lib_settings.path,
        doc_marker = doc_marker.path,
        gmp_libs = " ".join([f.path for f in ctx.files.gmp]),
        libz_libs = " ".join([f.path for f in ctx.files.libz]),
        bz2_libs = " ".join([f.path for f in ctx.files.bz2]),
        tinfo = ctx.file.tinfo.path,
    )

    ctx.actions.run_shell(
        outputs = [install_tree, lib_settings, doc_marker],
        inputs = depset(
            direct = ctx.files.srcs + [configure, ctx.file.make, ctx.file.tinfo] + ctx.files.gmp + ctx.files.libz + ctx.files.bz2,
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
            doc = "Hermetic libgmp.so(s) bundled into the rts libdir, e.g. @gmp//:libs.",
        ),
        "libz": attr.label(
            allow_files = True,
            doc = "Hermetic libz.so bundled into the rts libdir, e.g. @libz//:libs.",
        ),
        "bz2": attr.label(
            allow_files = True,
            doc = "Hermetic libbz2.so bundled into the rts libdir, e.g. @bzip2//:libs.",
        ),
        "tinfo": attr.label(
            allow_single_file = True,
            doc = "Hermetic libtinfo.so bundled into rts as libtinfo.so + libtinfo.so.5.",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
