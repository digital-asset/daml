# 002 - darwin/arm64 needs an LLVM 12 backend to run GHC 9.0.2

## Read this first

**This is not a regression against the old WORKSPACE/Nix build** — that build did
the exact same thing (it shipped an LLVM 12 alongside GHC on Apple Silicon). It is
only a "regression" **compared to how Linux and Windows behave**, where GHC needs
no LLVM at all. In other words: on x86_64 nothing extra is required; on arm64 we
have always had to carry an LLVM. We're just doing openly, in Bazel, what Nix used
to do quietly.

## The one-paragraph version

GHC's **native code generator (NCG) for AArch64 first shipped in GHC 9.2.1**. We
are on **GHC 9.0.2**, which has no arm64 NCG, so on Apple Silicon GHC falls back to
its **LLVM backend** (`-fllvm`) and shells out to `opt` and `llc`. GHC 9.0.2 only
accepts **LLVM 9–12**. Our hermetic C/C++ toolchain is **LLVM 22**, which GHC 9.0.2
cannot use (its version check refuses it, and the IR/pass-manager formats are
incompatible). So on darwin/arm64 we bundle a separate **LLVM 12 `opt`/`llc`**
purely to drive GHC's backend. Linux and Windows are x86_64, have the NCG, and are
completely unaffected.

## Why it only bites on arm64

| Platform | GHC codegen path | Needs LLVM `opt`/`llc`? |
|---|---|---|
| linux x86_64 | native NCG | no |
| windows x86_64 | native NCG | no |
| **darwin arm64 (GHC 9.0.2)** | **LLVM backend (`-fllvm`)** | **yes, LLVM 9–12** |

## Precedent: the WORKSPACE/Nix build did the same

Commits `f10a02eb61` ("Use GHC with LLVM on MacOS M1") and `eb65c4b8be` ("Select
ghcWithLLVM within Nix") wrapped `ghc`/`runghc`/`hsc2hs` to put
`pkgs.llvmPackages_12.clang` + `pkgs.llvmPackages_12.llvm` (i.e. `opt`/`llc`) on
`PATH`. `llvmPackages_12` is the only LLVM version that ever appears in
`nix/bazel.nix`. That commit's own message states it plainly: *"The native backend
only supports M1 starting from GHC 9.2. However, we are still on GHC 9.0."*

## What we do now

- Fetch LLVM 12 `opt`/`llc` + `libLLVM-12.dylib` as **native arm64-darwin
  conda-forge artifacts** (`llvm-tools` + `libllvm12`, pinned by sha256). There is
  no official LLVM 12 arm64-darwin release, and this Mac has no Rosetta, so conda's
  native arm64 build is the source. Everything else resolves from macOS system libs.
- The repo `@ghc_llvm_backend` provides those files.
- `ghc_bindist_install` **copies them into the GHC install tree** at
  `llvm-backend/{bin,lib}`, and the tool launchers prepend `llvm-backend/bin` to
  `PATH`. Because they live inside the install tree (which *is* the toolchain's
  `tools`), rules_haskell stages them into every Haskell action automatically —
  **no per-target wiring, no rules_haskell patch.** This mirrors the Nix
  `makeWrapper` PATH-prefix, baked into the tree.

## The switch (how to turn this off)

There is a single control, `DARWIN_GHC_LLVM_BACKEND` in
`bazel/versions/ghc.version.bzl` (marked with a large TODO banner).

**When GHC is bumped to >= 9.2 (which has the native AArch64 NCG), set it to
`False`.** That one flip disables the entire workaround: `@ghc_llvm_backend` fetches
nothing, nothing is copied into the toolchain, and the launchers stop touching
`PATH` — the toolchain reverts to its original, backend-free shape. Nothing else
needs to change. It is also fail-safe: if someone bumps GHC and forgets to flip it,
GHC >= 9.2 simply ignores the unused `opt`/`llc`.

## Files involved

- `bazel/versions/ghc.version.bzl` — the switch + the pinned conda artifacts.
- `bazel/haskell/toolchain/ghc_toolchain_extension.bzl` — the `@ghc_llvm_backend` repo.
- `bazel/haskell/toolchain/ghc_bindist_install.bzl` — copy into the install tree + launcher PATH.
- `bazel/haskell/toolchain/BUILD.bazel` — passes `llvm_backend` (gated on darwin + the switch).
- `MODULE.bazel` — `use_repo(..., "ghc_llvm_backend")`.

## Downstream issue this surfaced: merge-objects linker (RESOLVED)

Once the LLVM 12 backend was engaged, GHC reached its **"Merge objects"** link
phase (`ld -r`, a partial/relocatable link that combines a package's object files).
This is a linker problem **independent of the LLVM 12 backend** — it would bite the
GHC 9.2 NCG path too — and it needed two fixes, both `is_darwin`-gated:

1. **`ld.lld` is LLD's ELF frontend.** rules_haskell handed GHC's merge step
   (`-pgmlm`) the tool `cc_bindir/ld.lld`, which is the ELF linker and rejects
   *every* Mach-O object with `unknown file type`. (This is not a `-r`-support gap
   — that frontend simply cannot read Mach-O.) Fix: on darwin use the system
   `ld64` (`/usr/bin/ld`) for that tool. `ld64.lld` — LLD's Mach-O frontend — was
   rejected because its `-r` additionally demands `-arch` **and**
   `-platform_version`, which GHC does not supply; system `ld64` just works, which
   is consistent with the "accept the host Command Line Tools" boundary.
2. **macOS `ld64` needs an explicit `-arch`.** GHC's llc-produced objects lack the
   arch metadata the current linker uses to infer it, so `ld -r` fails with
   `ld: Missing -arch option`. Fix: add `-arch arm64` to the merge flags.

Where the fixes live (both darwin-only; Linux is untouched):
- `bazel/patches/haskell/rules_bazel-8_compat.patch` — sets `cc.tools.ld` to
  `/usr/bin/ld` on darwin. That tool feeds **only** `-pgmlm`, so nothing else
  changes; the main link still goes through the `cc` driver.
- `bazel/patches/haskell/rules_haskell-darwin-merge-objects-arch.patch` — adds
  `-arch arm64` in `ghc_cc_program_args`, which **both** the native
  (`toolchain.bzl`) and cabal (`cabal.bzl`) paths call. The toolchain
  `ghcopts`/`compiler_flags` attributes were not an option here: rules_haskell's
  own docs state cabal rules do not read them.

**These are NOT behind `DARWIN_GHC_LLVM_BACKEND`.** The merge linker is a permanent
darwin fix — GHC still merges objects with `ld -r` even after a bump to >= 9.2, so
this must stay regardless of the LLVM-12 switch.

## Current frontier: library-link wave

Past merge-objects, the build now fails one step later, at the **library-link**
step, with a fresh and separate wave:
- `HaskellLinkStaticLibrary` (and cabal) cannot resolve the `ar` tool:
  `llvm-ar: No such file or directory`, and cabal's `Cannot find the program 'ar'`
  pointing at a `_main/external/…/llvm-ar` runfiles-prefixed path that does not
  resolve in the darwin sandbox.
- `HaskellLinkDynamicLibrary` fails when GHC spawns `otool` for its Mach-O fixups:
  `otool: createProcess: posix_spawnp: illegal operation (Inappropriate ioctl for
  device)`.

These are the next step, tracked separately from the merge-objects fix above.
