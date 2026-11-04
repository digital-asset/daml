# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("//bazel/native:hermetic_cc.bzl", "TOOLBIN_SNIPPET", "hermetic_cc_flags")

_GHC_TOOLCHAIN = Label("//bazel/haskell/toolchain:ghc_toolchain_type")

_DARWIN_LD = """
for name in ld ld.lld; do
  rm -f "$TOOLBIN/$name"
  printf '#!/bin/sh\\nexec /usr/bin/ld "$@"\\n' > "$TOOLBIN/$name"
  chmod +x "$TOOLBIN/$name"
done
"""

_COMMAND = """\
set -euo pipefail

EXECROOT="$PWD"
SRC="$EXECROOT/$(dirname @@CABAL_PROJECT@@)"
STAGE="$EXECROOT/@@STAGE@@"
GHC="$EXECROOT/@@GHC@@"
CABAL="$EXECROOT/@@CABAL@@"
CLANG="$EXECROOT/@@COMPILER@@"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
export HOME="$TMP/home"
export CABAL_DIR="$TMP/cabal"
mkdir -p "$HOME"

cp -RpL "$SRC/." "$TMP/src"
chmod -R u+w "$TMP/src"
cd "$TMP/src"

sed -i.bak -e '/-- DELETE MARKER FOR CI/,/-- END DELETE/d' cabal.project
rm -f cabal.project.bak

cat > cabal.project.local <<'PROJECT'
tests: False
benchmarks: False
library-for-ghci: False
package *
  library-for-ghci: False
PROJECT

ZLIB="$TMP/zlib"
mkdir -p "$ZLIB/lib" "$ZLIB/include"
cp -L "$EXECROOT/@@ZLIB_ARCHIVE@@" "$ZLIB/lib/libz.a"
for header in @@ZLIB_HEADERS@@; do
  cp -L "$EXECROOT/$header" "$ZLIB/include/"
done
cat >> cabal.project.local <<PROJECT
package zlib
  extra-lib-dirs: $ZLIB/lib
  extra-include-dirs: $ZLIB/include
PROJECT

export PATH="/usr/bin:/bin:/usr/sbin:/sbin"
@@TOOLBIN@@
@@LD@@
cat > "$TOOLBIN/cc" <<WRAPPER
#!/bin/sh
exec "$CLANG" @@CFLAGS@@ @@LDFLAGS@@ "\\$@"
WRAPPER
chmod +x "$TOOLBIN/cc"
export PATH="$TOOLBIN:$(dirname "$GHC"):$PATH"

"$CABAL" update
"$CABAL" build exe:haskell-language-server -w "$GHC" -j
mkdir -p "$STAGE/bin" "$STAGE/lib"
cp "$("$CABAL" list-bin exe:haskell-language-server -w "$GHC")" "$STAGE/bin/haskell-language-server"
find "$CABAL_DIR/store" "$TMP/src/dist-newstyle" -name '*.dylib' -exec cp -L {} "$STAGE/lib/" \\;
"""

_BUNDLE_COMMAND = """\
set -euo pipefail
export PATH="/usr/bin:/bin"
STAGE="@@STAGE@@"
OUT="@@OUT@@"
LIBS="@@LIBS@@"
GHC="@@GHC@@"

mkdir -p "$LIBS"
cp "$STAGE/bin/haskell-language-server" "$OUT"
cp "$STAGE/lib/"*.dylib "$LIBS/"
chmod -R u+w "$OUT" "$LIBS"
chmod +x "$OUT"
for rpath in $(otool -l "$OUT" | awk '$1 == "cmd" { cmd = $2 } cmd == "LC_RPATH" && $1 == "path" { print $2 }'); do
  install_name_tool -delete_rpath "$rpath" "$OUT"
done
install_name_tool -add_rpath "@executable_path/../lib/@@GHC_VERSION@@" "$OUT"
codesign --force --sign - "$OUT"

GHC_LIBDIR="$("$GHC" --print-libdir)"
GHC_DYLIB_PATH=""
for dir in "$GHC_LIBDIR"/*/; do
  GHC_DYLIB_PATH="$GHC_DYLIB_PATH${dir%/}:"
done
DYLD_LIBRARY_PATH="${GHC_DYLIB_PATH%:}" "$OUT" --version
"""

def _static_library(target):
    cc_info = target[CcInfo]
    archives = [
        library.pic_static_library or library.static_library
        for linker_input in cc_info.linking_context.linker_inputs.to_list()
        for library in linker_input.libraries
        if library.pic_static_library or library.static_library
    ]
    if len(archives) != 1:
        fail("expected exactly one static library in {}, got {}".format(target.label, archives))
    headers = [h for h in cc_info.compilation_context.direct_public_headers if h.basename.endswith(".h")]
    return archives[0], headers

def _substitute(template, replacements):
    for key, value in replacements.items():
        template = template.replace(key, value)
    return template

def _hls_source_build_impl(ctx):
    ghc = ctx.toolchains[_GHC_TOOLCHAIN].ghc
    if ghc.version != ctx.attr.ghc_version:
        fail("HLS is built for GHC {}, but the toolchain is GHC {}".format(ctx.attr.ghc_version, ghc.version))
    cc_toolchain = find_cc_toolchain(ctx)
    cc = hermetic_cc_flags(ctx, cc_toolchain)
    zlib_archive, zlib_headers = _static_library(ctx.attr.zlib)

    stage = ctx.actions.declare_directory(ctx.label.name + "_cabal_build")
    ctx.actions.run_shell(
        outputs = [stage],
        inputs = depset(
            direct = ctx.files.srcs + [ctx.file.cabal_project, ctx.file.cabal, zlib_archive] + zlib_headers,
            transitive = [ghc.all_files, cc_toolchain.all_files],
        ),
        command = _substitute(_COMMAND, {
            "@@CABAL_PROJECT@@": ctx.file.cabal_project.path,
            "@@STAGE@@": stage.path,
            "@@GHC@@": ghc.ghc.path,
            "@@CABAL@@": ctx.file.cabal.path,
            "@@COMPILER@@": cc.compiler,
            "@@TOOLBIN@@": TOOLBIN_SNIPPET,
            "@@LD@@": _DARWIN_LD,
            "@@CFLAGS@@": cc.cflags,
            "@@LDFLAGS@@": cc.ldflags,
            "@@ZLIB_ARCHIVE@@": zlib_archive.path,
            "@@ZLIB_HEADERS@@": " ".join(['"{}"'.format(h.path) for h in zlib_headers]),
        }),
        mnemonic = "HlsCabalBuild",
        progress_message = "Building haskell-language-server from source with cabal (slow, needs network) for %{label}",
        execution_requirements = {
            "no-remote": "1",
            "requires-network": "1",
        },
        use_default_shell_env = False,
    )

    out = ctx.actions.declare_file("bin/haskell-language-server-" + ctx.attr.ghc_version)
    libs = ctx.actions.declare_directory("lib/" + ctx.attr.ghc_version)
    ctx.actions.run_shell(
        outputs = [out, libs],
        inputs = depset(direct = [stage], transitive = [ghc.all_files]),
        command = _substitute(_BUNDLE_COMMAND, {
            "@@STAGE@@": stage.path,
            "@@OUT@@": out.path,
            "@@LIBS@@": libs.path,
            "@@GHC@@": ghc.ghc.path,
            "@@GHC_VERSION@@": ctx.attr.ghc_version,
        }),
        mnemonic = "HlsBundle",
        progress_message = "Bundling haskell-language-server dylibs for %{label}",
        execution_requirements = {"no-remote": "1"},
        use_default_shell_env = False,
    )
    return [
        DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [libs])),
        OutputGroupInfo(libraries = depset([libs])),
    ]

hls_source_build = rule(
    implementation = _hls_source_build_impl,
    attrs = {
        "cabal": attr.label(
            default = Label("@cabal//:cabal"),
            allow_single_file = True,
            cfg = "exec",
        ),
        "cabal_project": attr.label(
            mandatory = True,
            allow_single_file = True,
        ),
        "ghc_version": attr.string(mandatory = True),
        "srcs": attr.label(mandatory = True, allow_files = True),
        "zlib": attr.label(
            default = Label("@libz//:libz"),
            providers = [CcInfo],
        ),
    },
    toolchains = [_GHC_TOOLCHAIN] + use_cc_toolchain(),
    fragments = ["cpp"],
)
