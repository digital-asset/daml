# Post-mortem — `bazel build //...` green on macOS arm64

Companion to `.bzlstatus/post-mortem.md` (the Linux campaign). Same structure,
scoped to the macOS/arm64 half.

## TL;DR

`bazelisk build //...` exits 0 on macOS arm64: **2096 targets, 0 failures**,
reproduced on a second fully-cached run. Eight `--keep_going` sweeps took it
from 7 distinct root causes to zero.

Two findings dominate; everything else was portability sediment.

1. **A code-signing invariant nobody states.** Apple's `install_name_tool` and
   `strip` silently re-sign a Mach-O *only* while it still carries the linker's
   own signature (`flags=0x20002 adhoc,linker-signed`). Once `codesign -f -s -`
   replaces it with a plain adhoc signature (`flags=0x2`), those same tools
   print a warning and leave the file **invalid** — and Apple Silicon then
   `SIGKILL`s any process that loads it, with no diagnostic anywhere in the
   build.
2. **Every "fix it in one place" bug had a second copy.** Three separate
   instances, each of which cost a full sweep to rediscover.

## The signing invariant, in detail

Symptom: three `HaskellCabalLibrary` actions died with
`Command '[... runghc ... Setup.hs build ...]' died with <Signals.SIGKILL: 9>`
and nothing else. No stderr, no linker error.

Evidence chain:

- `codesign -v` over the built tree: **130 of 207** `libHS*.dylib` invalid
  ("code or signature have been modified").
- The split was exact: every `0x20002 (linker-signed)` file was valid, every
  `0x2 (adhoc)` file was invalid.
- `/Library/Logs/DiagnosticReports/ghc-*.ips`:
  `exception.signal = "SIGKILL (Code Signature Invalid)"`,
  `termination.namespace = "CODESIGNING"`, `indicator = "Invalid Page"`,
  faulting thread inside `dyld4::APIs::dlopen_from`.
- Reproduced standalone: `strip -x` on a linker-signed dylib → still valid;
  `codesign -f -s -` then `strip -x` → invalid, with the warning.

Why only three packages: static linking never validates signatures. GHC only
`dlopen`s dependency dylibs when it has to **run** Template Haskell — which is
exactly `lsp-types`, `path-io` (via `path`'s quasi-quoters) and `parsers`.
`path`, which merely *defines* TH, built fine.

Two producers of the `0x2` state, so the first fix was necessary but not
sufficient:

| Producer | Then invalidated by |
|---|---|
| `cc_wrapper.py` re-signing after rewriting load commands | Cabal's `copy` running `--with-strip` |
| same | GHC's own `-pgminstall_name_tool` rpath injection |

**Fix chosen: stop creating the condition.** `cc_wrapper`'s
`install_name_tool`+`codesign` pair existed *only* to serve an rpath-shortening
optimisation in `darwin_shorten_rpaths`, which rewrote `LC_LOAD_DYLIB` entries
to `@rpath/<mangled-dir>/<lib>` so one `LC_RPATH` could replace several. The
dylibs already carry `@rpath/<basename>` install names, so
`rules_haskell-darwin-keep-linker-signature.patch` emits one rpath per directory
a dependency was actually found in and returns an empty rewrite list. Neither
tool runs, the linker signature survives, and Apple's tools keep self-re-signing
downstream. Result: **753/753 distinct dylibs valid**.

`rules_haskell-darwin-cabal-resign.patch` (re-sign every Mach-O under `pkgroot`
as the last step of `cabal_wrapper.py`) is retained as a safety net. It fixed
the Cabal family on its own (77/207 → 210/210) before the structural fix landed.

## The "second copy" pattern

| Concept | Copy A | Copy B | Cost |
|---|---|---|---|
| Absolutize execroot-relative toolchain flags | `hermetic_cc.bzl` `_PATH_PREFIXES` (fixed earlier, bug #5) | `ghc_lib_sdist.bzl` `_execroot_abs_flag` — missing `--sysroot=` | 1 sweep |
| `ghc-pkg` location in the darwin bindist | `compiler/damlc/util.bzl` `_GHC_PKG_UNIX` (fixed earlier, bug #4) | `Packaging.hs` `ghcPkgSubpath` — still `lib/lib/bin` | 1 sweep, 121 targets |
| Re-sign after modifying a Mach-O | `cabal_wrapper.py` | `haskell_library` link path | 1 sweep |

The `Packaging.hs` copy even carried the comment *"See `compiler/damlc/util.bzl`
for the corresponding Bazel-side label"* — the pointer existed, the sync did not.

Recorded as debt in
`.bzlremaining/improvements/duplicated-execroot-flag-absolutization.md`.

## The sediment (one line each)

| Fix | Was |
|---|---|
| `build_gnu_tool.bzl`, `configure_make.bzl` | `nproc` (absent) and GNU `sed -i` (BSD reads the next arg as a backup suffix) |
| `ghc_lib_sdist.bzl` | 4× GNU `sed -i`; `cp -rLt`; missing `chmod -R u+w` after copying read-only outputs |
| `daml_finance.BUILD.bzl` | GNU `tar --transform/--sort/--no-selinux`; now stages + uses `//bazel_tools/sh:mktgz` |
| `dpm.bzl` | `sha256sum` not on the action PATH |
| `package-app.sh`, `package-oci-component.sh` | `python` (gone since macOS 12.3) on the Darwin branch |
| `package-app.sh` | bundling `/usr/lib/*.dylib` — those live only in the dyld shared cache |
| `pkg-db/util.bzl` | `cp --remove-destination`; naive `cp -f` is *wrong* here (writes through the symlink) |
| `docs/BUILD.bazel`, `typedoc.bzl`, `ghc-lib*/BUILD.bazel` | GNU `sed -i` / `cp --no-preserve=mode -t`, fixed pre-emptively |
| `//bazel/haskell/toolchain:tinfo` | deb9 ncurses is Linux-only; gated `target_compatible_with`, `libncurses` added to the `@llvm` sysroot |

## Linux exposure

Everything is either Darwin-branch-only, constraint-gated, or
behaviour-preserving. Three items genuinely change Linux command lines and need
the Linux run to confirm:

- `make -j$(nproc)` → `make -j"$JOBS"`. The substitution failed *silently* on
  macOS only; on Linux this adds a fallback and nothing else.
- `sed -i` → `sed -i.bak … && rm -f`. Same result, one extra unlink.
- `@daml-finance` tarballs now come from hermetic bsdtar via `mktgz` rather than
  host GNU tar. Bytes differ; the sole consumer (`templates/BUILD.bazel`)
  untars them immediately, so only content matters.

`bazel/haskell/runtime_libs.bzl` centralises the runtime-lib path export. It
emits a **byte-identical** `LD_LIBRARY_PATH` line on Linux and a
`DYLD_LIBRARY_PATH` one on darwin — added so that gating `:tinfo` to Linux would
not silently turn seven `daml-script` targets incompatible-and-skipped on macOS,
which would have read as a false green.

## What to copy next time

- **Get the crash report.** `/Library/Logs/DiagnosticReports/*.ips` named the
  cause (`CODESIGNING / Invalid Page`) in one step, after a lot of speculation
  had gone nowhere.
- **Verify the artifact, not the action.** `codesign -v` across the output tree
  turned an intermittent-looking SIGKILL into a 130-of-207 census with an exact
  flag correlation.
- **Generate patches with `diff`.** Two hand-written hunk headers were short by
  one line; `patch` truncates the appended block silently and the result only
  fails much later.
- **When a fix has a "corresponding" comment, grep for the other side.**

## Out of scope

`bazel test //...` was never in scope (per `HANDOFF.md` §1). Build parity only.
