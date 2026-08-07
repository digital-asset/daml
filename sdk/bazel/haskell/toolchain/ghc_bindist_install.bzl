"""GHC bindist `./configure && make install` as a Bazel action (not fetch), so
the C compiler is the registered hermetic LLVM cc toolchain at action time."""

load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("//bazel/native:hermetic_cc.bzl", "TOOLBIN_SNIPPET", "hermetic_cc_flags")

# Launchers exec the real binary under <install>/lib/bin: the post-install
# `bin/` scripts hard-code the install prefix and are not relocatable across
# sandboxes, so `ghc`/`ghc-pkg` get an explicit `-B`/`--global-package-db`.
_LAUNCHER_EXTRA_ARGS = {
    "ghc": '-B"$LIBDIR"',
    "ghci": '--interactive -B"$LIBDIR"',
    "ghc-pkg": '--global-package-db "$LIBDIR/package.conf.d"',
    "runghc": '--ghc-arg=-B"$LIBDIR"',
    "haddock": '-B"$LIBDIR" -l"$LIBDIR"',
    "hsc2hs": '--template="$LIBDIR/template-hsc.h"',
}

_LAUNCHER_TARGET_BIN = {
    "ghci": "ghc",
}

# hsc2hs's bundled include must follow the caller's args, matching the stock wrapper.
_LAUNCHER_SUFFIX_ARGS = {
    "hsc2hs": '-I"$LIBDIR/include/"',
}
_TOOLS = ["ghc", "ghci", "ghc-pkg", "hsc2hs", "haddock", "runghc", "hpc"]

def _make_launcher(ctx, install_tree, tool_name, has_llvm_backend):
    launcher = ctx.actions.declare_file("{}_bin/{}".format(ctx.label.name, tool_name))
    llvm_path = 'export PATH="$ROOT/llvm-backend/bin:${PATH:-}"\n' if has_llvm_backend else ""
    ctx.actions.write(
        output = launcher,
        is_executable = True,
        content = """#!/usr/bin/env bash
set -euo pipefail
SELF="${{BASH_SOURCE[0]}}"
while [ -h "$SELF" ]; do
  d="$(cd -P "$(dirname "$SELF")" && pwd)"
  SELF="$(readlink "$SELF")"
  case "$SELF" in /*) ;; *) SELF="$d/$SELF" ;; esac
done
SELF="$(cd -P "$(dirname "$SELF")" && pwd)/$(basename "$SELF")"
ROOT="$(cd "$(dirname "$SELF")/../{tree}" && pwd)"
if [ -f "$ROOT/lib/lib/settings" ]; then LIBDIR="$ROOT/lib/lib"; else LIBDIR="$ROOT/lib"; fi
{llvm_path}exec "$ROOT/lib/bin/{tool}" {extra} "$@" {suffix}
""".format(
            tree = install_tree.basename,
            tool = _LAUNCHER_TARGET_BIN.get(tool_name, tool_name),
            extra = _LAUNCHER_EXTRA_ARGS.get(tool_name, ""),
            suffix = _LAUNCHER_SUFFIX_ARGS.get(tool_name, ""),
            llvm_path = llvm_path,
        ),
    )
    return launcher

def _sysroot_from_flags(cflags):
    toks = cflags.split(" ")
    for i in range(len(toks)):
        if toks[i] == "-isysroot" and i + 1 < len(toks):
            return toks[i + 1]
        if toks[i].startswith("--sysroot="):
            return toks[i][len("--sysroot="):]
    return None

def _ghc_bindist_install_impl(ctx):
    cc_toolchain = find_cc_toolchain(ctx)
    cc = hermetic_cc_flags(ctx, cc_toolchain)

    configure = ctx.file.configure
    install_tree = ctx.actions.declare_directory(ctx.label.name + "_install")

    # Sentinel so haskell_toolchain can infer a libdir_path; real libdir reaches
    # ghc via the launcher's `-B`.
    lib_settings = ctx.actions.declare_file("{}_layout/lib/settings".format(ctx.label.name))
    doc_marker = ctx.actions.declare_file(ctx.label.name + "_doc_marker")

    llvm_backend_files = ctx.files.llvm_backend
    has_llvm_backend = bool(llvm_backend_files)
    launchers = [_make_launcher(ctx, install_tree, tool, has_llvm_backend) for tool in _TOOLS]

    llvm_backend_snippet = ""
    if has_llvm_backend:
        lines = ['mkdir -p "$PREFIX/llvm-backend/bin" "$PREFIX/llvm-backend/lib"']
        for f in llvm_backend_files:
            if f.basename.endswith(".dylib"):
                lines.append('cp -L "$EXECROOT/{}" "$PREFIX/llvm-backend/lib/{}"'.format(f.path, f.basename))
            else:
                lines.append('cp -L "$EXECROOT/{}" "$PREFIX/llvm-backend/bin/{}"'.format(f.path, f.basename))
                lines.append('chmod +x "$PREFIX/llvm-backend/bin/{}"'.format(f.basename))
        llvm_backend_snippet = "\n".join(lines) + "\n"

    tinfo = ctx.file.tinfo
    runtime_lib_dirs = []
    for f in ([tinfo] if tinfo else []) + ctx.files.gmp + ctx.files.libz + ctx.files.bz2:
        d = "$EXECROOT/" + f.dirname
        if d not in runtime_lib_dirs:
            runtime_lib_dirs.append(d)

    tinfo_bundle = "" if not tinfo else """\
cp -L "$EXECROOT/{tinfo}" "$LIBDIR/rts/libtinfo.so"
cp -L "$EXECROOT/{tinfo}" "$LIBDIR/rts/libtinfo.so.5"
""".format(tinfo = tinfo.path)

    rts_bundle = "".join([
        'cp -L "$EXECROOT/{}" "$LIBDIR/rts/"\n'.format(f.path)
        for f in ctx.files.gmp + ctx.files.libz + ctx.files.bz2
    ])

    sysroot = _sysroot_from_flags(cc.cflags)
    ffi_fixup = "" if not sysroot else """\
FFI_SRC="{sysroot}/usr/include/ffi"
if [ -d "$FFI_SRC" ]; then
    find "$PREFIX" -name ffitarget.h | while read -r f; do
        d="$(dirname "$f")"
        for h in ffitarget_arm64.h ffitarget_armv7.h ffitarget_x86.h; do
            if [ -f "$FFI_SRC/$h" ] && [ ! -f "$d/$h" ]; then
                cp -f "$FFI_SRC/$h" "$d/"
            fi
        done
    done
fi
""".format(sysroot = sysroot)

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
export LD_LIBRARY_PATH="{ld_library_path}${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}}"
{toolbin}

./configure --prefix "$PREFIX"
JOBS="$( (nproc 2>/dev/null) || sysctl -n hw.ncpu 2>/dev/null || echo 1 )"
"$MAKE_BIN" -j"$JOBS" install

if [ -f "$PREFIX/lib/lib/settings" ]; then LIBDIR="$PREFIX/lib/lib"; else LIBDIR="$PREFIX/lib"; fi

{llvm_backend_snippet}
{ffi_fixup}
{rts_bundle}
{tinfo_bundle}
sed -i.bak \
    -e 's#("C compiler command", "[^"]*")#("C compiler command", "cc")#' \
    -e 's#("Haskell CPP command", "[^"]*")#("Haskell CPP command", "cc")#' \
    "$LIBDIR/settings"
rm -f "$LIBDIR/settings.bak"

cp "$LIBDIR/settings" "$EXECROOT/{lib_settings}"
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
        ld_library_path = ":".join(runtime_lib_dirs),
        toolbin = TOOLBIN_SNIPPET,
        make = ctx.file.make.path,
        lib_settings = lib_settings.path,
        doc_marker = doc_marker.path,
        rts_bundle = rts_bundle,
        tinfo_bundle = tinfo_bundle,
        ffi_fixup = ffi_fixup,
        llvm_backend_snippet = llvm_backend_snippet,
    )

    ctx.actions.run_shell(
        outputs = [install_tree, lib_settings, doc_marker],
        inputs = depset(
            direct = ctx.files.srcs + [configure, ctx.file.make] + ([tinfo] if tinfo else []) + ctx.files.gmp + ctx.files.libz + ctx.files.bz2 + llvm_backend_files,
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
        "llvm_backend": attr.label(
            allow_files = True,
            doc = "darwin/arm64 only: LLVM 12 opt/llc + libLLVM copied into llvm-backend/ for GHC's -fllvm. See DARWIN_GHC_LLVM_BACKEND.",
        ),
    },
    toolchains = use_cc_toolchain(),
    fragments = ["cpp"],
)
