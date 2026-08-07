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

## Known follow-up (separate issue)

With the LLVM 12 backend engaged, GHC now advances to its **"Merge objects"** link
phase, which its settings run as `ld.lld -r`. LLD's Mach-O port does not properly
support relocatable (`-r`) links, so this fails with `ghc_N.o: unknown file type`.
That is a **linker-configuration** problem, independent of the LLVM 12 backend
(it would bite the NCG path too), and is tracked as the next step — the merge-objects
tool needs to be a Mach-O-`-r`-capable linker (system `ld64`), not `ld.lld`.
