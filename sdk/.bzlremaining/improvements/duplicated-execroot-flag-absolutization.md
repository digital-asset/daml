# Duplicated execroot flag absolutization

## What

From-source rules `cd` into a temp build dir before invoking the C compiler.
The cc-toolchain emits execroot-relative flags (`external/...`,
`bazel-out/...`), so each rule rewrites them to absolute paths first. Two
independent implementations:

| File | Symbol | Absolute form |
|---|---|---|
| `bazel/native/hermetic_cc.bzl` | `_PATH_PREFIXES` / `_abs()` | `$EXECROOT/...`, shell-expanded at action time |
| `bazel/haskell/ghc_lib_sdist.bzl` | `_execroot_abs_flag()` | `__EXECROOT__/...`, `sed`-substituted at action time |

Prefix lists differ:

- `hermetic_cc.bzl`: `-L -B -I -F -iquote --sysroot=`
- `ghc_lib_sdist.bzl`: `-isystem -iquote -idirafter -I -L -B -F --sysroot=` plus
  a `""` catch-all for bare relative tokens

## Failure mode

On macOS `@llvm` emits both sysroot forms. `-isysroot <path>` is two tokens, so
the bare path hits the catch-all. `--sysroot=external/...` is one token and
needs its own prefix entry. Without it the sysroot stays relative and overrides
the correct absolute `-isysroot` for `ld64.lld`'s library search:

```
ld64.lld: error: library not found for -lSystem
ld64.lld: error: library not found for -lc++
ld64.lld: error: framework not found for -framework Foundation
configure: error: C compiler cannot create executables
```

`ghc_lib_sdist.bzl` deletes its build dir via `trap ... EXIT`, so `config.log`
is not preserved on failure.

Latent on Linux: `sysroot_flags` is `empty_sysroot_flags` there, so no
`--sysroot` token is emitted.

## Status

| Copy | Fixed in |
|---|---|
| `hermetic_cc.bzl` | earlier session; `--sysroot=` added to `_PATH_PREFIXES` |
| `ghc_lib_sdist.bzl` | `d40ed942d2` |

Not unified: merging the lists changes Linux flag handling, since
`hermetic_cc.bzl` handles neither `-isystem`/`-idirafter` nor bare relative
tokens.

## Remaining work

One helper holding the prefix list and rewrite, parameterised by absolute
prefix (`$EXECROOT` vs `__EXECROOT__`), adopted in both rules under Linux
verification. Any further from-source rule that `cd`s before compiling uses it
rather than a third copy.
