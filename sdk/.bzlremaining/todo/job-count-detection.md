# `-j` in the from-source build rules

`JOBS_SNIPPET` in `bazel/native/hermetic_cc.bzl` (and the identical expression
inline in `bazel/haskell/toolchain/ghc_bindist_install.bzl`) reads:

```sh
JOBS="$( (nproc 2>/dev/null) || sysctl -n hw.ncpu 2>/dev/null || echo 1 )"
```

and is used as `"$MAKE" -j"$JOBS"`.

There are two questions here, and the second one matters more than the first.

## 1. Should these rules pass `-j` at all?

**Not obviously.** GNU Make never inspects the CPU count. From `make --help`
(GNU Make 4.4.1):

```
-j [N], --jobs[=N]          Allow N jobs at once; infinite jobs with no arg.
```

So the options are: no `-j` → strictly serial; bare `-j` → unbounded; `-j N` →
exactly N. There is no `-j auto`, and `-l/--load-average` is the only
load-sensitive knob.

The catch is that **Bazel is already the scheduler**, and these actions do not
tell it what they cost:

- `.bazelrc:49` — `build --local_resources=cpu="HOST_CPUS*.5"`, i.e. 10 on a
  20-core host.
- None of `configure_make`, `build_gnu_tool`, `ghc_bindist_install` or
  `ghc_lib_sdist` set `resource_set` or `execution_requirements`, so Bazel
  assumes **1 CPU per action**.

Bazel may therefore run ~10 of these concurrently while each forks `-j20`:
up to ~200 processes on 20 cores. Bazel exposes no jobserver to actions, so the
nested makes cannot coordinate with it or with each other.

Three ways out:

| Option | Effect | Cost |
|---|---|---|
| **a.** Drop `-j`; let Bazel's cross-action parallelism carry throughput | No oversubscription, simplest | Long single actions become critical path — sweep 6's critical path was already 2106s, much of it GHC |
| **b.** Declare the cost: `resource_set = lambda os, inputs: {"cpu": N}` and pass the same `N` to `-j` | Correct accounting, keeps intra-action parallelism | Most work; N still has to come from somewhere |
| **c.** Status quo | Fastest when few actions are runnable | Oversubscribed when many are |

(a) or (b) are both defensible; (c) is what we have and is the least principled.
Worth measuring before choosing — the dependency chain `make → m4 → autoconf →
automake` is largely serial anyway, so the oversubscription window may be
narrower in practice than the worst case above.

## 2. If we keep `-j N`, the detection is fragile

It degrades silently in **both** directions.

**Silently serial.** `sysctl` lives in `/usr/sbin`, which is not on every action
PATH — Bazel genrules on this host run with `PATH=/bin:/usr/bin:/usr/local/bin`.
There, macOS has no `nproc` *and* cannot reach `sysctl`, so the chain falls
through to `echo 1` and builds run `-j1`: correct, but serial, with no
diagnostic. This is latent only because `configure_make` / `build_gnu_tool` run
under `env -`, where bash's default PATH happens to include `/usr/sbin`. It goes
live as soon as either rule is invoked from a narrower-PATH action.

**Silently unbounded.** `-j"$JOBS"` is a single shell word, so an empty result
collapses it to plain `-j` — reintroducing exactly the unbounded parallelism
that replacing `-j$(nproc)` was meant to remove.

If a number is still wanted, `getconf _NPROCESSORS_ONLN` is POSIX, lives in
`/usr/bin` on both macOS and Linux, and needs no fallback chain (returns `20`
here). Any fallback that remains should **fail loudly** rather than default to
`1` or to empty.

## Also: the expression is duplicated

- `bazel/native/hermetic_cc.bzl` — `JOBS_SNIPPET`
- `bazel/haskell/toolchain/ghc_bindist_install.bzl` — inline copy

Same shape as
`.bzlremaining/improvements/duplicated-execroot-flag-absolutization.md`, where
one concept in two copies cost a full build sweep to rediscover. Whatever is
decided above should land in one place.
