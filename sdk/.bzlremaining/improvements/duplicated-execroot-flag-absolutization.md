# Duplicated execroot flag absolutization

## Context

Several from-source rules `cd` into a temporary build directory before invoking
the C compiler. Bazel's cc-toolchain emits execroot-*relative* flags
(`external/...`, `bazel-out/...`), so each of those rules rewrites them to
absolute paths before handing them to `./configure` and friends.

That rewrite exists **twice**, in two independent implementations:

| Location | Symbol | Absolute form |
|---|---|---|
| `bazel/native/hermetic_cc.bzl` | `_PATH_PREFIXES` / `_abs()` | `$EXECROOT/...` (shell, expanded at action time) |
| `bazel/haskell/ghc_lib_sdist.bzl` | `_execroot_abs_flag()` | `__EXECROOT__/...` (placeholder, `sed`-substituted at action time) |

The prefix lists are **not** the same:

- `hermetic_cc.bzl`: `-L -B -I -F -iquote --sysroot=`
- `ghc_lib_sdist.bzl`: `-isystem -iquote -idirafter -I -L -B -F --sysroot=` + a
  `""` catch-all for bare relative tokens

## Why this matters

The identical bug had to be found and fixed twice. On macOS the `@llvm`
toolchain emits **both** `--sysroot={path}` (one token) and `-isysroot {path}`
(two tokens). The two-token form is absolutized by the bare-path rule; the
`--sysroot=` form is a single token and needs its own prefix entry. Missing it
leaves a relative `--sysroot`, which overrides the correct absolute `-isysroot`
for `ld64.lld`'s library search, and the only symptom is:

```
ld64.lld: error: library not found for -lSystem
ld64.lld: error: library not found for -lc++
ld64.lld: error: framework not found for -framework Foundation
configure: error: C compiler cannot create executables
```

Fixed in `hermetic_cc.bzl` first (recorded as bug #5 in `.bzlmigration/plan.md`),
then again, independently, in `ghc_lib_sdist.bzl`.

Latent on Linux either way: there `sysroot_flags` is `empty_sysroot_flags`, so
no `--sysroot` token is ever emitted.

## Why it was not unified now

Merging the two lists would change flag handling on the Linux path
(`hermetic_cc.bzl` currently handles neither `-isystem`/`-idirafter` nor bare
relative tokens) for no macOS benefit, and the macOS work is under a hard
"Linux must stay green" constraint.

## Suggested improvement

Move the prefix list and the rewrite into one shared helper, parameterised by
the absolute prefix (`$EXECROOT` vs `__EXECROOT__`), and adopt it in both rules
in a Linux-verified change of its own. Any third from-source rule that needs to
`cd` before compiling should use that helper rather than a third copy.
