# Accepted host-tool dependencies on darwin

macOS arm64 is deliberately not hermetic in the way Linux is: the build uses the
host Xcode Command Line Tools SDK as the cc sysroot. This is the resulting
host-tool list.

## Accepted

| Tool | Where | Reason |
|---|---|---|
| `/usr/bin/{ld,ar,nm,strip}` | `rules_bazel-8_compat.patch`, darwin branch | Mach-O `ld -r` merge-objects has no `lld` equivalent for GHC's `-pgmlm`. Linux keeps `llvm-*` from `@llvm`. |
| `/usr/bin/{install_name_tool,otool}` | `rules_haskell-darwin-cc-wrapper-otool.patch` | `@llvm`'s minimal toolchain ships no Mach-O equivalents. |
| `/usr/bin/codesign` | `rules_haskell` `cc_wrapper.py` fallback; `package-app.sh` | No hermetic substitute for ad-hoc signing. |
| CLT SDK `libncurses` | `llvm_osx.libraries` in `MODULE.bazel` (`104cf03adf`) | macOS has no libtinfo; the deb9 ncurses 5.9 build is Linux-only. |
| `/usr/lib`, `/System/Library` libraries | `package-app.sh` `is_in_dyld_shared_cache` (`c4087a8712`) | In the dyld shared cache, no file on disk to bundle. Reference kept absolute. |
| `readlink -f` | `package-app.sh`, `package-oci-component.sh`, darwin branch (`bd9ab767a3`) | Replaced a `python` shim. Needs macOS 12.3+; toolchain pins `-mmacosx-version-min=14.0`. Build-host only; the shipped wrapper's `readlink -f` is in the Linux branch. |

## Still hermetic on darwin

cc toolchain (`@llvm` 22.1.8), GHC 9.0.2 bindist, `make`, `m4`, `autoconf`,
`automake`, `perl`, `python`, `tar` — all from-source or bindist, same as Linux.

## Invariant

`--with-strip` resolving to CLT `strip` is what makes the Mach-O signature bug
reachable.

`install_name_tool` and `strip` re-sign a Mach-O only while it carries the
linker's signature (`flags=0x20002 adhoc,linker-signed`). After
`codesign -f -s -` replaces it with plain adhoc (`flags=0x2`), both warn and
leave the file invalid; Apple Silicon SIGKILLs any process that loads it.

Any step modifying a Mach-O after linking must preserve the linker-signed flag
or re-sign last. See `3c5c5c8d72`.
