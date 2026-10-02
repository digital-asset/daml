# `-j` in the from-source build rules

`JOBS_SNIPPET` in `bazel/native/hermetic_cc.bzl`, duplicated inline in
`bazel/haskell/toolchain/ghc_bindist_install.bzl`:

```sh
JOBS="$( (nproc 2>/dev/null) || sysctl -n hw.ncpu 2>/dev/null || echo 1 )"
```

Used as `"$MAKE" -j"$JOBS"`. Unresolved as of `2a0d6d9d06`.

## Whether `-j` belongs here at all

GNU Make never inspects CPU count. From `make --help` (4.4.1):

```
-j [N], --jobs[=N]          Allow N jobs at once; infinite jobs with no arg.
```

No `-j auto`. Without `-j`: serial. Bare `-j`: unbounded. `-l/--load-average`
is the only load-sensitive knob.

Bazel is already the scheduler and these actions do not declare their cost:

- `.bazelrc:49` — `build --local_resources=cpu="HOST_CPUS*.5"` (10 on a 20-core
  host)
- `configure_make`, `build_gnu_tool`, `ghc_bindist_install`, `ghc_lib_sdist` set
  neither `resource_set` nor `execution_requirements`, so Bazel assumes 1 CPU
  per action

Worst case: ~10 concurrent actions × `-j20` = ~200 processes on 20 cores. Bazel
exposes no jobserver to actions, so nested makes cannot coordinate.

| Option | Effect | Cost |
|---|---|---|
| Drop `-j` | No oversubscription | Long actions become critical path; sweep 6 critical path was 2106s, mostly GHC |
| `resource_set = {"cpu": N}` + `-j N` | Correct accounting | Most work; N still needs a source |
| Status quo | Fastest when few actions runnable | Oversubscribed when many are |

Unmeasured. The `make → m4 → autoconf → automake` chain is largely serial, so
the real concurrency window may be narrower than the worst case.

## Detection fragility, if `-j N` stays

Degrades silently in both directions.

Serial: `sysctl` is `/usr/sbin/sysctl`; `/usr/sbin` is absent from some action
PATHs (Bazel genrules run with `PATH=/bin:/usr/bin:/usr/local/bin`). There
macOS has no `nproc` and cannot reach `sysctl`, so the chain yields `1`. Latent
only because `configure_make` / `build_gnu_tool` run under `env -`, where bash's
default PATH includes `/usr/sbin`.

Unbounded: `-j"$JOBS"` is one shell word. An empty result collapses it to `-j`,
reintroducing the unbounded parallelism that replacing `-j$(nproc)` removed.

`getconf _NPROCESSORS_ONLN` is POSIX, in `/usr/bin` on both platforms, needs no
fallback chain, returns `20` on this host. Any remaining fallback should fail
rather than default to `1` or empty.

## Duplication

- `bazel/native/hermetic_cc.bzl` — `JOBS_SNIPPET`
- `bazel/haskell/toolchain/ghc_bindist_install.bzl` — inline copy

Same pattern as
`.bzlremaining/improvements/duplicated-execroot-flag-absolutization.md`.
