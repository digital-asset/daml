# Verify a running `daml-prebuilt` sandbox

**For the agent reading this:** you are (or should be) running **inside** a sandbox launched from the
`daml-prebuilt` template, typically via `sbx run --clone -t daml-prebuilt … claude sdk`. Your job is to
**verify the image works as designed** and **report back** — do not "fix" anything unless asked.

The image is supposed to give you, for free (in read-only layers, off the 20 GB overlay):
1. daml's **nix dev-env** already realized (so `dade assist` is instant, no downloads),
2. a **primed bazel repository cache** (external deps),
3. a **fully-baked bazel disk cache** (action outputs) — so a `bazel build` is near-all cache hits
   with no recompilation, and the 20 GB overlay stays mostly free.

Two design assumptions need empirical confirmation here (they couldn't be tested at authoring time):
**(A)** the disk cache was actually populated, and **(B)** a runtime build *hits* it (i.e. the
`--config=linux` cache-key parity holds). Checks 4 and 5 are the ones that prove these.

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
echo "== ~/.bazelrc (should point at the baked caches + force --config=linux) =="; cat ~/.bazelrc
```
- **Expect:** both dirs exist; **`disk` is multi-GB** (this is assumption A — a near-empty disk cache
  means the build-time backfill failed); `~/.bazelrc` contains `--disk_cache=/opt/daml-bazel-cache/disk`,
  `--repository_cache=/opt/daml-bazel-cache/repo`, and `--config=linux`.
- **If `disk` is tiny (< ~1 GB):** assumption A failed — report it; runtime builds will recompile.

## 4. THE key test — does a build hit the cache? (assumption B)

First relocate the writable output base off the 20 GB overlay. **In `--clone` mode use `--vdc`** (the
clone lives on the overlay, so the default host-disk target has nowhere roomy to go):

```bash
daml-bazel-prepare --vdc        # plain `daml-bazel-prepare` if this is a host-mounted (non-clone) sandbox
```

Now build a representative, non-trivial target and read the process summary:

```bash
bazel build //compiler/damlc:damlc 2>&1 | tee /tmp/build1.log | tail -25
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
  that means the runtime action keys don't match the baked cache (assumption B broken; usually a
  `--config` mismatch — confirm `~/.bazelrc` from check 3 forces `--config=linux`).
- **Expect:** the build finishes **fast** (minutes, mostly I/O) and **succeeds**.

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
