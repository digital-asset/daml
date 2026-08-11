# Accepted host-tool dependencies on darwin

The macOS arm64 build is deliberately **not** hermetic in the way the Linux
build is. `.bzlmigration/HANDOFF.md` locked that decision (host Xcode Command
Line Tools SDK as the cc sysroot). This file records the host tools that
decision actually pulls in, so they are owned debt rather than accidents.

## Accepted

| Tool | Where | Why |
|---|---|---|
| `/usr/bin/{ld,ar,nm,strip}` | `bazel/patches/haskell/rules_bazel-8_compat.patch` (darwin branch) | Mach-O `ld -r` merge-objects has no drop-in `lld` equivalent for GHC's `-pgmlm`. The Linux side keeps the `llvm-*` tools from `@llvm`. |
| `/usr/bin/{install_name_tool,otool}` | `bazel/patches/haskell/rules_haskell-darwin-cc-wrapper-otool.patch` | `@llvm`'s minimal toolchain ships no Mach-O equivalents. |
| `/usr/bin/codesign` | `rules_haskell` `cc_wrapper.py` fallback, and `bazel_tools/packaging/package-app.sh` | Ad-hoc signing has no hermetic substitute. |
| CLT SDK `libncurses` | `llvm_osx.libraries(names = [...])` in `MODULE.bazel` | macOS has no libtinfo; the deb9 ncurses 5.9 build is Linux-only. |
| OS libraries under `/usr/lib`, `/System/Library` | `package-app.sh` `is_in_dyld_shared_cache` | They live in the dyld shared cache and have no file on disk to bundle; the reference is kept absolute. |

## Consequence worth remembering

`--with-strip` resolving to CLT `strip` is what made the Mach-O signature bug
reachable at all. Apple's `install_name_tool`/`strip` silently re-sign a Mach-O
only while it still carries the linker's own signature (`flags=0x20002
adhoc,linker-signed`); once `codesign -f -s -` has replaced it with a plain
adhoc signature (`flags=0x2`), those tools warn and leave the file **invalid**,
and Apple Silicon then SIGKILLs any process that loads it.

The fix keeps the linker signature instead of re-signing — see
`bazel/patches/haskell/rules_haskell-darwin-keep-linker-signature.patch` and the
WP-5 section of `.bzlmigration/plan.md`. Any future step that modifies a Mach-O
after linking must either preserve the linker-signed flag or re-sign as its very
last action.

## Not accepted / still hermetic on darwin

- cc toolchain: `@llvm` 22.1.8, same as Linux.
- GHC 9.0.2: hermetic `aarch64-apple-darwin` bindist.
- `make`, `m4`, `autoconf`, `automake`, `perl`, `python`, `tar`: all from-source
  or bindist, same as Linux.
