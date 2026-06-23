# Verify a running `daml-prebuilt` sandbox

**For the agent reading this:** you are (or should be) running **inside** a sandbox launched from the
`daml-prebuilt` template, typically via `sbx create --clone -t daml-prebuilt … claude .` then
`sbx run --name …` (run from the daml repo **root**, since `--clone` needs the git repo). Your job is to
**verify the image works as designed** and **report back** — do not "fix" anything unless asked.

The image is supposed to give you, for free (in read-only layers, off the 20 GB overlay):
1. daml's **nix dev-env** already realized (so `dade assist` is instant, no downloads),
2. a **primed bazel repository cache** (external deps),
3. a **fully-baked bazel disk cache** (action outputs) — so a `bazel build` is near-all cache hits
   with no recompilation, and the 20 GB overlay stays mostly free.

Two design assumptions need empirical confirmation here (they couldn't be tested at authoring time):
**(A)** the disk cache was actually populated, and **(B)** a runtime build *hits* it (i.e. the
`--config=linux` cache-key parity holds). Checks 4 and 5 are the ones that prove these.

**Previously-needed runtime workarounds — now baked into the image (verify they're in effect, don't
re-apply):** an earlier full-build test surfaced three issues that are now fixed at image-build time.
You should find them already handled:
- **Repo cache writable** (Bazel 7 writes temp files in the SHA dirs even on hits): the Dockerfile now
  `chown`s `/opt/daml-bazel-cache/repo` to `agent`. Verify with `ls -ld /opt/daml-bazel-cache/repo`
  (owner `agent`) — no `sudo chmod` should be needed.
- **TMPDIR vs the hermetic sandbox** (Claude Code sets `TMPDIR=/tmp/claude-<uid>`, which Bazel 7's
  hermetic-tmp sandbox overmounts away): `daml-bazel-prepare` now detects a non-`/tmp` `$TMPDIR` and
  adds `build --sandbox_add_mount_pair=$TMPDIR` to its managed block, so a plain `bazel build` works —
  no `TMPDIR=/tmp` prefix needed.
- **Full output materialization** (the baked disk cache lacks some intermediates and daml's remote CAS
  evicts, so build-without-the-bytes fails): `daml-bazel-prepare` now emits
  `build --remote_download_outputs=all` (not `=toplevel`). Verify it's in `.bazelrc.local` after
  running `daml-bazel-prepare`.

(Also relevant to a clean build: the cloned worktree's `sdk/bazel-haskell-deps.bzl` must have a
non-empty `GRPC_HASKELL_SHA256` so the archive is read from the baked repo cache offline. This is a
daml-source fix carried in via `--clone`, not part of the image.)

Run the checks in order, from the **`sdk/` directory** (your starting cwd). Record actual output.

---

## 0. Context — where am I, and what was baked

```bash
echo "sandbox: ${SANDBOX_VM_ID:-?}  host: $(hostname)"
cat /etc/daml-prebuilt.ref 2>/dev/null && echo "  <- baked daml ref" || echo "NOT a daml-prebuilt image (/etc/daml-prebuilt.ref missing)"
pwd; ls -1 WORKSPACE .bazelrc 2>/dev/null            # expect to be in sdk/ (WORKSPACE + .bazelrc present)
git rev-parse --show-toplevel                         # repo root (the PARENT of sdk/ — .git lives there)
git -C "$(git rev-parse --show-toplevel)" rev-parse HEAD
```
- **Expect:** `/etc/daml-prebuilt.ref` prints a 40-char SHA; cwd is `…/sdk`; `WORKSPACE` + `.bazelrc`
  exist; repo root resolves to the parent of `sdk/`.
- **If `/etc/daml-prebuilt.ref` is missing:** you're not in a daml-prebuilt sandbox — stop and report.

## 1. Disk baseline (capture BEFORE building)

```bash
echo "== overlay (the 20G limit) ==";        df -h /
echo "== local vdc disk (/var/lib/docker) =="; df -h /var/lib/docker 2>/dev/null || echo "no vdc"
```
- **Expect:** `/` (overlay) shows **low usage** (a fresh sandbox is ~1–2 GB used of 20 GB). Note the
  "Used" figure — you'll compare after the build (check 5).

## 2. Baked nix dev-env

```bash
[ -S /nix/var/nix/daemon-socket/socket ] && echo "nix daemon: up" || echo "nix daemon: DOWN (should auto-start at boot)"
du -sh /nix/store 2>/dev/null                         # baked closure — expect multiple GB
time eval "$(dev-env/bin/dade assist)"                # MUST be fast and quiet
command -v bazel; bazel --version                     # daml's nix bazel wrapper, 7.x
```
- **Expect:** daemon up; `/nix/store` is several GB; `dade assist` returns in a few seconds **without
  a wall of `downloading`/`building` lines** (that would mean the closure wasn't baked or the checkout
  drifted). `bazel` resolves to a `dev-env/bin` path and reports a 7.x version.
- **If `dade assist` downloads/builds heavily:** the nix closure isn't being hit — note it (likely a
  ref/nixpkgs drift between this checkout and the baked ref from check 0).

## 3. Baked bazel caches

```bash
ls -ld /opt/daml-bazel-cache/disk /opt/daml-bazel-cache/repo
du -sh  /opt/daml-bazel-cache/disk /opt/daml-bazel-cache/repo
echo "== ~/.bazelrc (should point at the baked caches) =="; cat ~/.bazelrc
```
- **Expect:** both dirs exist; **`disk` is multi-GB** (this is assumption A — a near-empty disk cache
  means the build-time backfill failed); `~/.bazelrc` contains `--disk_cache=/opt/daml-bazel-cache/disk`
  and `--repository_cache=/opt/daml-bazel-cache/repo`. It should **not** set `--config=linux` — the
  nix wrapper `dev-env/bin/bazel` injects that itself; a second copy would double `:linux` flags and
  break protoc.
- **If `disk` is tiny (< ~1 GB):** assumption A failed — report it; runtime builds will recompile.

## 4. THE key test — does a build hit the cache? (assumption B)

First relocate the writable output base off the 20 GB overlay. **In `--clone` mode use `--vdc`** (the
clone lives on the overlay, so the default host-disk target has nowhere roomy to go):

```bash
daml-bazel-prepare --vdc        # plain `daml-bazel-prepare` if this is a host-mounted (non-clone) sandbox
```

Build with a **plain `bazel build`** — no `TMPDIR=/tmp` prefix and no `sudo chmod` (those workarounds
are now baked, see the pre-flight note above). Start with a representative target as a quick smoke
test, then **run the full `//...` build** — that is the real validation (it's the build that originally
surfaced every cache bug), so don't skip it:

```bash
bazel build //compiler/damlc:damlc 2>&1 | tee /tmp/build1.log | tail -25   # quick smoke test
bazel build //...                  2>&1 | tee /tmp/build-all.log | tail -25   # FULL build — the real check
```
- **Watch during analysis:** there should be **little or no** `Downloading`/`Fetching` of external
  repos (that's the repo cache working).
- **Read the final `INFO: N processes:` line.** Cache is working when it is dominated by
  **`disk cache hit`** (and/or `remote cache hit`), with very few `linux-sandbox` / `worker` / `local`
  (those are actual (re)executions). Example of a PASS:
  ```
  INFO: 2473 processes: 2470 disk cache hit, 3 internal.
  ```
  A FAIL looks like hundreds/thousands of `linux-sandbox` or `worker` processes (real recompilation) —
  that means the runtime action keys don't match the baked cache (assumption B broken). The usual cause
  is a `--config` mismatch: the nix wrapper injects `--config=linux` exactly once; do not add another
  copy (in `~/.bazelrc` or on the command line) — that would double `:linux` flags and break protoc.
- **No `CacheNotFoundException`** (that was the BwoB bug, now fixed by `--remote_download_outputs=all`)
  and **no protoc `--include_source_info may only be passed once`** (the double-`--config=linux` bug).
  If either appears, the image wasn't rebuilt from the fixed Dockerfile — report it.
- **Expect:** the full build finishes (mostly I/O on a warm cache) and **succeeds** end-to-end.

Optional second build to confirm warm incrementality (should be near-instant, "0 processes" or all
cached):
```bash
bazel build //compiler/damlc:damlc 2>&1 | tail -3
```

## 5. Did the overlay stay free? (the whole point)

```bash
echo "== overlay AFTER the build =="; df -h /
echo "== vdc AFTER the build ==";     df -h /var/lib/docker 2>/dev/null
```
- **Expect:** `/` (overlay) "Used" grew only **marginally** vs check 1 (the baked caches are read-only
  layers; the output base went to vdc). Most build bytes should land on **vdc**, not `/`.
- **If `/` jumped by several GB:** the output base or cache writes are hitting the overlay — likely
  `daml-bazel-prepare` wasn't applied (re-check check 4) or `--vdc` was needed but plain mode was used.

## 6. Git / commit sanity (for later work)

```bash
root="$(git rev-parse --show-toplevel)"
git -C "$root" status -s | head
git -C "$root" remote -v | head
git config user.name && git config user.email || echo "GIT IDENTITY NOT SET — commits need it + DCO -s"
```
- **Expect:** a clean-ish clone whose `origin` points at the daml repo. If identity is unset, note it
  (daml requires `git commit -s` with a configured name/email).

## 7. Drift note (informational)

```bash
echo "baked ref:   $(cat /etc/daml-prebuilt.ref)"
echo "checkout at:  $(git -C "$(git rev-parse --show-toplevel)" rev-parse HEAD)"
```
- If these differ a lot, expect check 4 to show **some** real recompilation (the delta) on top of the
  cache hits — that's normal, not a failure.

---

## Report back

Summarize PASS/FAIL with the concrete numbers, especially:

| # | Check | Key evidence to quote |
|---|-------|-----------------------|
| 2 | nix baked | `dade assist` time + whether it downloaded |
| 3 | disk cache populated (assumption A) | `du -sh /opt/daml-bazel-cache/disk` |
| 4 | build hits cache (assumption B) | the `INFO: N processes: … disk cache hit …` line |
| 5 | overlay stayed free | `/` Used before vs after |

State clearly whether **assumptions A and B both hold**. If either fails, quote the evidence and the
likely cause (A: backfill/`bazel-cache.da-ext.net` at build time; B: `--config` key mismatch) — but do
not change anything unless the user asks.
