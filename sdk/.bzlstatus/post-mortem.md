# Post-mortem — "Making GHC (somewhat) hermetic" on the LLVM pivot

Scope: the `wojciechormaniec-da/module-migration/4/llvm-pivot` effort to move the
Daml SDK off host-tool/Bootlin-GCC wiring onto a hermetic LLVM toolchain, and to
build all Haskell (damlc + the whole `//...` graph) without host `cc`, host
glibc, or host shared libs.

Outcome (Linux `linux_amd64`): **`bazel build //...` → 2092/2092, exit 0.** The
build is code-complete on Linux from a clean cache. Windows/macOS and full
*runtime* hermeticity remain separate, unfinished tasks.

---

## TL;DR

The hard, recurring, time-dominating problem was not the LLVM toolchain itself —
clang/lld/compiler-rt + a pinned glibc-2.28 sysroot behaved. **The problem was
`rules_haskell`.** Its toolchain model silently assumes a *non-hermetic host*:
that a working `cc`, a host libc/headers/CRT, and host shared libraries are
present to fill gaps. The hermetic LLVM toolchain's flags
(`--sysroot=/dev/null -nostdlibinc -isystem <glibc headers> -B <crt> -L <glibc>
-rtlib=compiler-rt -fuse-ld=lld`) only reach GHC through **one** of the several
code paths by which GHC/Cabal actually invoke a C compiler. Every other path
leaked out to the (absent) host, so each surfaced as a distinct hard failure.

We closed the gaps with three `rules_haskell` patches plus a long tail of
per-site runtime-library fixes. None of it is upstreamed; all of it is a fork we
now carry.

---

## What we set out to do

- Rip out the Bootlin/GCC toolchain wiring and `gcc_toolchain_extension`.
- Wire the BCR **`llvm`** module (not `toolchains_llvm`) as a hermetic-first
  cc toolchain, bzlmod-only.
- Make hermeticity a hard requirement at both fetch time and action time
  (`.bzlmigration/overview.md`, "Hermeticity Criteria").
- Keep the SDK's own Haskell (GHC 9.0.2 bindist → damlc etc.) building under
  that toolchain.

The dev host deliberately has **no `cc`/`gcc`/`clang`** and sets
`BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1`. That is the whole point — it turns every
latent host-fallback in the build into a loud, reproducible failure instead of a
silent non-hermetic success.

### Context that matters: this was also a de-Nix / WORKSPACE→bzlmod migration

The 220-commit history shows the toolchain swap was one part of a much larger
move: **WORKSPACE → bzlmod** and the **complete removal of Nix** (`@ghc_nix`,
`@nix_ghc_lib_deps`, `@hlint_nix`, `@jq_dev_env`, `@sysctl_nix`, `script_nix`,
the `dev_env_tool`/dadew provisioning — all gone; the only "nix" left in build
config is dead comments). This is the load-bearing context for everything below:

> **Nix was the SDK's previous hermeticity mechanism.** It supplied GHC, a C
> compiler, system libraries, and dev tools as a curated environment, so
> `rules_haskell`'s own host-assumptions never bit — they were masked by Nix.
> Removing Nix didn't *create* the `rules_haskell` gaps; it **exposed** them.
> The SDK was "hermetic-by-Nix," not "hermetic-by-rules_haskell."

Consequences visible in the history that reinforce this:
- First-party mechanisms built for the old GCC/Nix world had to be reworked —
  e.g. `bazel_tools/fat_cc_library.bzl` (the static-lib bundler) was **deleted**
  and replaced by `bazel/native/hermetic_cc.bzl` + `configure_make.bzl`.
- The GCC path had its own host-libc divergence (`/lib64/libm.so.6`,
  `libmvec.so.1`; see `.bzlregression/issues/001_libm_libmvec_sysroot_divergence.md`)
  that the LLVM pivot ultimately resolved by controlling the sysroot outright.
- A huge amount of the churn (yarn→pnpm/`rules_js`, maven/scala/go/proto
  re-wiring) is bzlmod-migration breadth unrelated to the Haskell thesis and is
  intentionally out of scope for this document.

---

## The central finding: `rules_haskell` assumes a host that isn't there

GHC is not a self-contained compiler in Bazel's sense; it shells out to a C
compiler in **many** places, and it also *is* shelled out to by Cabal. A
hermetic cc toolchain only works if its flags reach **every** one of those
invocations. `rules_haskell` plumbs them through some and not others:

| C-invocation path | Who invokes it | Did rules_haskell forward hermetic flags? |
|---|---|---|
| GHC compile/link via `-optc/-optP/-optl` | native `haskell_library`/`haskell_binary` | **Yes** (`compile.bzl`/`link.bzl` from `cc.compiler_flags/cpp_flags/linker_flags`; hsc2hs gets `--cflag/--lflag`) |
| Cabal `Setup configure`/build | `stack_snapshot` → `cabal_wrapper.py` | **No** — forwarded only the `-pgm*` program *pointers*, not the sysroot flags |
| Package `./configure` scripts | Cabal (build-type Custom/Configure) | **No** — no `--configure-option=CFLAGS/LDFLAGS/CPPFLAGS` |
| Cabal `checkForeignDeps` probe | Cabal, bare `--with-gcc` | **No** — bare `cc`, GHC's `-optc/-optl` don't apply |
| `c2hs` / `hsc2hs` C preprocessing | rules_haskell's own `c2hs`/hsc2hs rules | **Partial / No** (c2hs went straight to bare clang) |
| GHC's **own** link-time C stub (`ghc_<n>.c`, includes `Rts.h`→`inttypes.h`) | GHC internally, via `-pgmc` = `cc_wrapper` | **No** — `cc_wrapper` was a passthrough that injected nothing |
| Fetch-time GHC bindist install (`./configure && make install`) | the `bindists()` repo rule | **No** — ran on host `cc`/`make`/libs at *fetch* time |

On a normal machine the "No" rows work anyway, because bare `cc` finds
`/usr/include`, `/usr/lib`, `crt1.o`, `-lc`, `libz.so`, etc. On our hermetic
host they are the failures. The toolchain abstraction is **leaky**: hermetic C
config should flow through a single choke point, but `rules_haskell` scatters C
invocation across ≥6 paths with inconsistent forwarding and relies on host
fallback to paper over the rest.

This is the same point as the Nix context above, stated at the code level: the
"No" rows are exactly the fallbacks Nix used to satisfy. `rules_haskell` never
had to be self-sufficiently hermetic because the surrounding environment always
was. Take the environment away and the leaks are load-bearing.

---

## The concrete inadequacies (and what each cost us)

### 1. Fetch-time bindist install needs host `cc`/`make`/libs — dead on arrival
`rules_haskell@1.0` `bindists(version="9.0.2")` runs `./configure && make
install` **at fetch time on host PATH**. With no host `cc` this is
`no acceptable C compiler found in $PATH` before anything else can happen
(memory: `no-host-cc-stock-ghc-bindist-fails`). Fetch-time repo rules also can't
cleanly consume registered Bazel toolchains.

**What we did:** move the install to an **action-time** rule that uses
`find_cc_toolchain` (hermetic clang/lld) + hermetic `make`, `RelocatableBuild =
YES`, `use_default_shell_env = False`. Because Bazel needs GHC's ~34 built-in
package metadata at *analysis* time but it only exists *after* install, we
resolved the chicken-and-egg with a committed, generated
`ghc_packages.lock.json` (`pkgdb_to_lock.py` on hermetic CPython) and **dropped
`pkgdb_to_bzl`** (whose `find_python` shelled out to host python) — a net
hermeticity gain. Design: `.bzlmigration/hermetic-ghc-toolchain.md`.

### 2. The Cabal path dropped the sysroot flags
`stack_snapshot` → `cabal_wrapper.py` forwarded only `-pgm*`. Result: every
Cabal C compile/link fell back to host headers/libs — fine on the dev host,
fatal in the minimal container (`string.h not found`, `cannot create
executables`, `cannot open Scrt1.o`, `-lc not found`).

**What we did:** `rules_haskell-cabal-cc-sysroot.patch` — thread
`cc.compiler_flags/cpp_flags/include_args/linker_flags` into
`_cabal_toolchain_info`, and in `cabal_wrapper.py` forward them **per action
only** as `--ghc-option=-optP/-optc/-optl`, `--hsc2hs-option=--cflag/--lflag`,
and the package's own `./configure` via `--configure-option=CFLAGS/LDFLAGS`.
Two hard-won gotchas, both recorded in memory `cabal-cc-sysroot-hermeticity`:
- **Never** mutate `os.environ` CFLAGS/LDFLAGS — Cabal re-applies them to *every*
  package's link and a `--sysroot=/dev/null` leak broke ~169 packages including
  pure-Haskell ones. Scoped options only.
- The path-absolutiser must guard `tok.startswith(pre)`, or `-I` (checked before
  `-B`/`-L`) rewrites `-Lpath`→`-Ipath` and silently kills library search / CRT.

### 3. Bare `cc` invocations that bypass GHC entirely
Even after (2), a cluster kept failing because they never go through GHC's
`-optc/-optl` at all: Cabal `checkForeignDeps`, package `./configure`, `c2hs`
(grpc-haskell), and **GHC's own link-time stub compile**. The common root:
`cc_wrapper` injected nothing, so bare-`cc` calls got no `-isystem`/`-B`/`-L`/
`-rtlib` (memory: `cabal-cc-wrapper-flag-leak`).

**What we did:** `rules_haskell-cc-wrapper-inject.patch` — bake the toolchain's
full compile **and** link command lines
(`cc_common.get_memory_inefficient_command_line`) into `cc_wrapper` and forward
them on *every* invocation. Because every C call runs through `cc_wrapper`, this
fixes all four leak points at once.

### 4. The exec-vs-target config leak (a Bazel × rules_haskell trap)
`cc_wrapper` is built in the **exec** config, so the baked link flags embed
`bazel-out/k8-opt-exec-*/bin/...` paths that don't exist in the **target**-config
action sandboxes → naive baking regressed *everything* with `ld.lld: cannot
open`. Fix: at runtime resolve `external/...` against `RULES_HASKELL_EXEC_ROOT`
and resolve generated `bazel-out/<cfg>/bin/...` by **globbing** `bazel-out/*/bin/
<rest>` for whichever config tree is actually staged (see the `.py.tpl` half of
the inject patch). This is pure incidental complexity from rules_haskell baking
config-specific paths into a wrapper shared across configs.

### 5. No RPATH / runfiles propagation for hermetic shared libs
The hermetic `libz.so`, `libgmp.so`, `libtinfo.so.5`, `libbz2.so` are linked
with **no RPATH**, and `rules_haskell`/the toolchain don't propagate a runtime
search path to the many genrules, `ctx.actions.run` rules, and tool executions
that run a zlib/gmp/tinfo-using Haskell binary (damlc, `compile-proto-file`,
`generate-stable-package`, doc generators, …). Every wave surfaced more
`error while loading shared libraries: … cannot open shared object file`
(Exit 127).

**What we did (per-site, by explicit user decision):** `LD_LIBRARY_PATH`
prepends in genrules; `runtime_lib_dirs` attrs + `LD_LIBRARY_PATH` on
`daml_package_rule` (`util.bzl`), `_daml_build` / `_inspect_dar` /
`generate_and_track_yaml_file` (`rules_daml/daml.bzl`); `extra_lib_dirs`
(→ `PACKAGE_APP_EXTRA_LIB_DIRS`) on `package_app` targets (daml-new-dpm,
damlc-dist, damlc-dist-dpm). This recurs on every new call site — it is a
symptom of a missing systemic fix (model `-lz`/`-lgmp`/`-ltinfo`/`-lbz2` as
proper libs so RPATH/runfiles propagate, or give the toolchain a real sysroot).

### 6. Bazel 8 API incompatibility
`rules_haskell@1.0` used `ctx.resolve_tools` and other Bazel-7-isms broken on
Bazel 8 across toolchain/cabal/protobuf/c2hs/plugin paths →
`rules_bazel-8_compat.patch`.

All three patches are registered via `single_version_override` in `MODULE.bazel`
(lines 46–52).

### 7. There are *two* GHCs to make hermetic
Easy to miss: the 9.0.2 **bindist toolchain** (items 1–6) is only one of them.
The DAML GHC fork (`@da-ghc` → `ghc-lib` / `ghc-lib-parser`) is **built from
source** as a vendored sdist (`bazel/haskell/ghc_lib_sdist.bzl`,
`da_ghc_extension.bzl`) via `ghc-lib-gen` + `./boot` + `./configure` + hadrian +
`deriveConstants`. That inner build re-hits the *same* class of host-tool leaks
independently of `rules_haskell`, and each needed its own fix: an inner `cc`
wrapper injecting the sysroot/link flags (mirroring patch 3), `-fuse-ld=lld` on
a bare `clang -shared`, unprefixed binutils symlinks (`ranlib`→`llvm-ar`),
hermetic `python3` for `./boot`, `LD_LIBRARY_PATH` for `deriveConstants`' libgmp,
and the `environ` `LD_PRELOAD` shim (below). Lesson: "make GHC hermetic" is
really "make every GHC-shaped build in the graph hermetic."

---

## Adjacent problems the hermeticity push *surfaced* (not rules_haskell's fault)

- **@llvm glibc `environ`/`__environ` alias split.** The curated `@llvm`
  glibc-2.28 `libc.so.6` defines `environ` and `__environ` as two distinct
  objects instead of a weak alias. Prebuilt `libHSbase`'s `__hscore_environ` +
  `-no-pie` COPY-reloc then leaves the `environ` symbol NULL → `getEnvironment`
  returns empty and `System.Process` passes an empty env to children. Root fix
  belongs in the `@llvm` libc curation; we carry two workarounds (a Haskell
  `dlsym` fixup in the client/server runner, an `LD_PRELOAD` ctor shim before
  `ghc-lib-gen`). Memory: `hermetic-glibc-environ-alias-broken`. This is a
  *hermetic-glibc build* bug that only a hermetic GHC could expose.

- **Container cache volume `:U` + Bazel tree artifacts.** `compose.yml` mounts
  the cache as `bazel-cache:/home/host/.cache:U`; `:U` re-chowns the whole
  volume every container start, invalidating Bazel tree artifacts
  (`declare_directory` outputs) so they re-run every build and then can't delete
  their stale read-only trees (`Permission denied`) under the rootless userns.
  Purely environmental; a clean-volume build reaches 2092. Memory:
  `bazel-cache-volume-U-delete-eperm`.

---

## Hermeticity boundary — why "somewhat"

- **Build/link is hermetic:** everything compiled/linked through the LLVM cc
  toolchain resolves against LLVM's bundled glibc-2.28 sysroot.
- **Not (yet) hermetic at runtime:** produced binaries still resolve
  `libc.so.6` from the host loader (`/lib64/ld-linux-x86-64.so.2`), and the
  prebuilt deb9 `ghc`/`ghc-pkg` binaries are host-linked and can't be relinked.
  LLVM glibc is a build/ABI sysroot, not a runtime-shipped libc.
- **Linux only.** Windows (relocatable mingw bindist, different path) and macOS
  (Apple toolchain + `install_name_tool`/codesign) are unproven separate tasks.

---

## Cost & fragility

- We now **fork `rules_haskell`** via three patches that must be re-verified on
  every `rules_haskell` upgrade; none are upstreamed.
- The RPATH/`LD_LIBRARY_PATH` fixes are **per-site** and **recur** on every new
  Haskell-tool call site — a standing maintenance tax the team pays each time
  the build graph grows, by explicit decision to defer the root fix.
- Two live workarounds depend on an external `@llvm` glibc bug being left
  unfixed.

---

## What actually worked (the model to copy)

Not everything was a workaround. The **gmp** and **ncurses/tinfo** libraries
were rebuilt as **pure `cc_library` targets compiled on the LLVM glibc-2.28
sysroot** (`bazel/haskell/toolchain/files/gmp.BUILD.bzl` + committed
`config.h`/handauthored patch), retiring the earlier `configure_make`/
`rules_foreign_cc` path. This was a *real* hermeticity gain, and it caught a
subtle leak the old path hid: the `configure_make` gmp had been compiling
against **host** glibc headers (`__isoc23_*@GLIBC_2.38`), forcing an ugly
`--allow-shlib-undefined`. The pure-`cc_library` build floors at `GLIBC_2.17`
with zero `__isoc23` refs.

The contrast is the whole argument: where a dependency is modeled as a proper
Bazel `cc_library` on the hermetic sysroot, hermeticity is *structural* and
flags/RPATH propagate for free. Where it isn't — the per-site `LD_LIBRARY_PATH`
tail (item 5) — it's whack-a-mole. Recommendation 3 is simply "do more of what
worked for gmp."

---

## Recommendations

1. **Push the flag-plumbing upstream, or lobby for a single cc choke point.**
   The real defect is that `rules_haskell` has no single place where hermetic C
   flags are guaranteed to apply. The `cc_wrapper`-inject approach is the
   closest thing to that choke point and is the best upstream candidate.
2. **Prefer giving `@llvm` a real, unified `--sysroot`** so a bare `clang` works
   without per-caller plumbing. This is the "most-correct, bigger" alternative
   noted during the work and would make patches (2), (3), and (4) largely
   unnecessary.
3. **Do the systemic shared-lib fix** (model `-lz`/`-lgmp`/`-ltinfo`/`-lbz2` as
   real libs with propagated RPATH/runfiles) to end the `LD_LIBRARY_PATH`
   whack-a-mole — it would also fix ghci/repl and any future tool.
4. **Fix the `@llvm` glibc `environ` alias** and retire both env workarounds.
5. **Change the `compose.yml` `:U` mount** (bind-mount a host dir owned by the
   keep-id user) so incremental container rebuilds stop breaking on tree-artifact
   deletion.
6. **Decide, deliberately, whether `rules_haskell` is the right long-term
   substrate.** The fork burden and the depth of its host assumptions are real
   inputs to that decision; hermetic Haskell-on-Bazel is not a supported path
   today, and this migration is essentially a proof that it *can* be forced to
   work — at a carrying cost.

---

## Appendix — key artifacts

- Patches: `bazel/patches/haskell/rules_haskell-cabal-cc-sysroot.patch`,
  `…/rules_haskell-cc-wrapper-inject.patch`, `…/rules_bazel-8_compat.patch`
  (registered in `MODULE.bazel` `single_version_override`, lines 46–52).
- Action-time GHC install + package lock design:
  `.bzlmigration/hermetic-ghc-toolchain.md`; lock `ghc_packages.lock.json`,
  generator `bazel run //bazel/haskell/ghc:ghc_packages.pin`.
- Per-site runtime-lib fixes: `compiler/damlc/pkg-db/util.bzl`,
  `rules_daml/daml.bzl`, `compiler/damlc/BUILD.bazel`,
  `daml-script/{test,daml}/BUILD.bazel`, `docs/BUILD.bazel`, proto/stable-package
  genrules, `daml-assistant/daml-new/BUILD.bazel`.
- Migration control panel & decision log: `.bzlmigration/overview.md`.
- Session state / build recipe: `.bzlmigration/dump.md`.
- Distilled root causes (agent memory): `cabal-cc-sysroot-hermeticity`,
  `cabal-cc-wrapper-flag-leak`, `no-host-cc-stock-ghc-bindist-fails`,
  `hermetic-glibc-environ-alias-broken`, `bazel-cache-volume-U-delete-eperm`.
