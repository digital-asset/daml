# Post-mortem — `bazel build //...` on macOS arm64

Companion to `.bzlstatus/post-mortem.md` (Linux campaign).

## Outcome

`bazelisk build //...` exits 0: 2096 targets, 0 failures, stable across a
second fully-cached run. `bazel test //...` was not in scope.

## Finding 1 — Mach-O code-signing invariant

`install_name_tool` and `strip` re-sign a Mach-O only while it carries the
linker's signature (`flags=0x20002 adhoc,linker-signed`). After
`codesign -f -s -` replaces it with plain adhoc (`flags=0x2`), both tools warn
and leave the file invalid. Apple Silicon SIGKILLs any process that loads it.

Symptom: `HaskellCabalLibrary` actions dying with `died with <Signals.SIGKILL: 9>`
and no other output.

Evidence:

- `codesign -v` over the output tree: 130/207 `libHS*.dylib` invalid.
- Split exact: every `0x20002` valid, every `0x2` invalid.
- `/Library/Logs/DiagnosticReports/ghc-*.ips` —
  `signal = "SIGKILL (Code Signature Invalid)"`,
  `termination.namespace = "CODESIGNING"`, `indicator = "Invalid Page"`,
  faulting thread `dyld4::APIs::dlopen_from`.
- Standalone repro: `strip -x` on linker-signed → valid;
  `codesign -f -s -` then `strip -x` → invalid.

Scope: static linking never validates signatures. GHC `dlopen`s dependency
dylibs only to *run* Template Haskell, so only `lsp-types`, `path-io` (via
`path`'s quasi-quoters) and `parsers` failed. `path`, which only defines TH,
built.

Producers of the `0x2` state:

| Producer | Invalidated by |
|---|---|
| `cc_wrapper.py` re-signing after rewriting load commands | Cabal `copy` running `--with-strip` |
| same | GHC `-pgminstall_name_tool` rpath injection |

Fix `3c5c5c8d72` — `rules_haskell-darwin-keep-linker-signature.patch`.
`cc_wrapper`'s `install_name_tool`+`codesign` pair existed only to serve an
rpath-shortening optimisation in `darwin_shorten_rpaths` (rewriting
`LC_LOAD_DYLIB` to `@rpath/<mangled-dir>/<lib>` so one `LC_RPATH` replaces
several). Dylibs already carry `@rpath/<basename>` install names, so the patch
emits one rpath per directory a dependency was found in and returns an empty
rewrite list. `darwin_rewrite_load_commands` short-circuits on its existing
`if args:`; neither tool runs. Result: 753/753 distinct dylibs valid.

`rules_haskell-darwin-cabal-resign.patch` re-signs every Mach-O under `pkgroot`
as `cabal_wrapper.py`'s last step. Fixed the Cabal family alone (77/207 →
210/210). Redundant once keep-linker-signature landed; retained as fallback.

Upstream deviation in keep-linker-signature: the original filtered out
libraries with absolute install names before adding an rpath; the patch adds it
whenever anything was found. Costs one or two redundant `LC_RPATH` entries and
widens the basename-collision surface. No failures or collisions observed.

## Finding 2 — one concept, two copies

| Concept | Fixed copy | Stale copy | Cost |
|---|---|---|---|
| Absolutize execroot-relative flags | `hermetic_cc.bzl` `_PATH_PREFIXES` | `ghc_lib_sdist.bzl` `_execroot_abs_flag`, missing `--sysroot=` | 1 build round |
| darwin `ghc-pkg` location | `compiler/damlc/util.bzl` `_GHC_PKG_UNIX` | `Packaging.hs` `ghcPkgSubpath`, `lib/lib/bin` | 1 build round, 121 targets |
| Re-sign after modifying a Mach-O | `cabal_wrapper.py` | `haskell_library` link path | 1 build round |

`Packaging.hs` carried a comment referring to `compiler/damlc/util.bzl` as its
counterpart.

See `.bzlremaining/improvements/duplicated-execroot-flag-absolutization.md`.

## Fix inventory

| Commit | Change |
|---|---|
| `004c3acc12` | `nproc` → `$JOBS`; GNU `sed -i` → `sed -i.bak` in `build_gnu_tool.bzl`, `configure_make.bzl` |
| `2a0d6d9d06` | `-j` handling flagged unresolved |
| `e8a7d54743` | `sha256sum`, `cp --remove-destination`, `cp --no-preserve=mode -t`, GNU `sed -i` in `docs/`, `typedoc.bzl` |
| `323e4bc1eb` | `@daml-finance` tarballs: `tar --transform` → staging + `mktgz` |
| `bd9ab767a3` | `python` shims removed from `package-app.sh`, `package-oci-component.sh` |
| `c4087a8712` | `package-app.sh`: skip dyld-shared-cache libraries when bundling |
| `104cf03adf` | tinfo gated to Linux; `libncurses` into `@llvm` sysroot; `runtime_libs.bzl` |
| `3c5c5c8d72` | Mach-O signing patches |
| `d40ed942d2` | `ghc_lib_sdist.bzl`: `--sysroot=`, `cp -rLt`, `chmod -R u+w`, `environ` shim and `-optl-no-pie` gated to Linux |
| `0083165a29` | `Packaging.hs` darwin `ghc-pkg` path |

## Linux-affecting changes

Three items change Linux command lines. Everything else is Darwin-branch-only,
constraint-gated, or output-preserving.

| Change | Linux effect |
|---|---|
| `make -j$(nproc)` → `make -j"$JOBS"` | None. Substitution failed only on macOS; Linux gains a fallback. |
| `sed -i` → `sed -i.bak … && rm -f` | Same output, one extra unlink per file. |
| `@daml-finance` tarballs via `mktgz` | Bytes differ: ordering no longer pinned by `--sort=name`, owner/group 1000 → 0. Sole consumer `templates/BUILD.bazel` untars immediately. |

`bazel/haskell/runtime_libs.bzl` centralises the runtime-lib path export:
byte-identical `LD_LIBRARY_PATH` on Linux (checked against every original
string), `DYLD_LIBRARY_PATH` on darwin. Required because gating `:tinfo` to
Linux would otherwise make seven `daml-script` targets incompatible and
silently skipped on macOS.

## Open items

| Item | Reference |
|---|---|
| `-j` handling | `.bzlremaining/todo/job-count-detection.md` |
| Flag-absolutization duplication | `.bzlremaining/improvements/duplicated-execroot-flag-absolutization.md` |
| Accepted host tools | `.bzlremaining/improvements/darwin-host-tool-hermeticity.md` |
| `is_darwin` (host detection) drives target-affecting decisions in `runtime_libs.bzl`, `bazel/haskell/toolchain/BUILD.bazel` | `.bzlremaining/todo/remove-os-info.md` |
