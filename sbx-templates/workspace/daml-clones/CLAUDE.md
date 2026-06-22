# daml sandbox environment

This clone is the [digital-asset/daml](https://github.com/digital-asset/daml) repo. Its dev
environment lives under the **`sdk/`** subdirectory — run all build/test commands from `sdk/`. If
you're not there, `cd` to it first (the dir with `WORKSPACE`/`.envrc`).

> **`git` says "not a git repository"?** daml's `.git` is at the **repo root** (the parent of `sdk/`).
> `sbx` mounts only the path it was pointed at — if it was pointed straight at `sdk/`, the repo-root
> `.git` was left outside the sandbox. The sandbox must be launched with the **repo root** as the
> workspace (then `cd sdk`), not `sdk/` itself. `bazel build` works without `.git`, but `git`,
> `build.sh`, and commits don't.

## Load the dev-env first (no direnv needed)

daml's tools (bazel, jdk, scala, …) come from its `dev-env`. Normally direnv auto-loads them via
`.envrc`, but **this sandbox has no direnv hook**, so don't rely on `direnv allow` — load the env
yourself. From `sdk/`:

```bash
eval "$(dev-env/bin/dade assist)"     # puts bazel, jdk, scala, node, python, … on PATH; sets locale + NIX_PATH
```

- This is exactly what `.envrc` does (minus `use nix`). The whole nix closure is baked into read-only
  layers, so it's fast — nothing to download or build.
- It affects **only the current shell**, so run it again in each fresh shell (e.g. at the start of a
  command sequence). For a single one-off command you can skip it: `dev-env/bin/bazel build //...`
  self-bootstraps the dev-env on its own.
- Minimal alternative: `export PATH="$PWD/dev-env/bin:$PATH"` — bazel works, but you'll see a harmless
  `setlocale: LC_ALL … en_US.UTF-8` warning (this skips dade's locale setup; daml's `bazel` wrapper
  still sets the locale for the build itself).
- The `bazel` you get is daml's nix wrapper around `bazel_7` (bazelisk pins 7.1.0). Don't install
  bazel yourself and don't run `nix develop` directly.

## When asked to build: run these immediately, no exploration needed

```bash
eval "$(dev-env/bin/dade assist)"     # load the dev-env (see above) — once per shell
daml-bazel-prepare                    # once per checkout: move bazel's output base onto host disk (see "Disk" below)
bazel build //compiler/damlc:damlc
```

## Disk — bazel artifacts vs the 20G overlay (READ THIS)

The sandbox has a **fixed 20 GB writable overlay** that can't be enlarged. The nix toolchain is baked
into read-only layers (free of the overlay), but bazel's **output base** (built artifacts) must be
writable — by default it goes to `~/.cache/bazel` on the overlay, and a daml build overflows it. That
is the "out of space" failure.

**Fix: run `daml-bazel-prepare` once** (from `sdk/`) before building. It writes `.bazelrc.local` so
that:
- bazel's output base goes to `sdk/.bazel-cache/output` on the **host-mounted** workspace disk (roomy),
  not the overlay;
- `--remote_download_outputs=toplevel` (build-without-the-bytes) pulls intermediates from daml's
  remote cache instead of materializing them locally;
- `--sandbox_base=/tmp/daml-bazel-sandbox` + `--reuse_sandbox_directories` keep the per-action
  **sandbox symlink forest on the fast local overlay** instead of churning it over the slow
  host mount (virtiofs). Artifacts still live on host disk, so builds are still a bit slower than a
  pure-overlay build — that's the cost of not being capped at 20 GB. If it's still too slow, add
  `build --spawn_strategy=local` to `.bazelrc.local` (drops the per-action sandbox entirely — faster,
  but less hermetic).

For the remote cache to help, the host must allow it: `sbx policy allow network bazel-cache.da-ext.net`.
(This relocation only helps when the workspace is **host-mounted** — plain host mount or host-clone
mode. In pure `sbx run --clone` the workspace itself is on the overlay and there's no roomy disk to
move to.)

**Faster: `daml-bazel-prepare --vdc`.** The host mount is virtiofs (slow). The sandbox also has a
**local** 50 GB disk at `/var/lib/docker` (the nested-Docker data disk, usually near-empty) — local
means *fast*, no virtiofs penalty. `--vdc` points the output base there (`/var/lib/docker/daml-bazel-out`)
instead of the host mount, giving fast **and** roomy. It needs passwordless sudo (to add `o+x` on
`/var/lib/docker` and create an agent-owned dir) and is best when you're **not** using nested Docker
in this sandbox. Caveats: shared with any nested-Docker images, and it only persists for the sandbox's
**lifetime** (a recreate wipes it — the default host-disk mode is the persistent one). Re-running
`daml-bazel-prepare` (no flag) switches back to host disk; the helper rewrites its managed block in
`.bazelrc.local` either way.

## Full vs scoped builds

Once `daml-bazel-prepare` has moved the output base to host disk, a **full build is allowed** —
`bazel build //...` or daml's own `./build.sh`. The 20 GB overlay is no longer the limit; what
matters now is host free space and time, and daml's remote cache makes most of `//...` cache hits
(with build-without-the-bytes, intermediates aren't even materialized locally).

**Scoping is an optimization, not a rule** — build just what you need when you want it faster / lighter
(with the dev-env loaded, per "Load the dev-env first"):

```bash
bazel build //...                              # whole SDK
bazel build //ledger-service/http-json:http-json  # one target
bazel build //compiler/damlc/...                  # one subtree
```

If you do fill the disk, reclaim it with `bazel clean` (or `bazel clean --expunge` to drop the whole
output base).

## Caches — what's baked vs fetched

- **Toolchain (nix):** baked into the image. Present at boot, no download.
- **Bazel external deps (`--repository_cache`):** primed into a read-only baked cache (wired via
  `~/.bazelrc`). Pre-baked deps are cache hits; a genuinely new dep downloads fresh (bazel logs that
  it can't write the read-only cache, then proceeds — that warning is expected).
- **Built action outputs:** daml's **remote cache** (`bazel-cache.da-ext.net`, pull-only) serves them
  if its host is allowed in the sandbox network policy. Otherwise actions build locally into the
  overlay `--disk_cache=.bazel-cache/disk`. If the remote cache is blocked, ask the user to run on the
  HOST: `sbx policy allow network bazel-cache.da-ext.net`.

## Tests

`--test_output=errors` is already the default. Scope tests the same way as builds — never run the
whole suite.

```bash
bazel test //daml-lf/data:data-test
```

If asked to run tests without a specific target named, ask the user which target to run.

## Before submitting a PR

Format first, then verify nothing is left (dev-env loaded):

```bash
./fmt.sh                 # scalafmt, hlint, java/yapf, prettier, etc.
```

If `fmt.sh` reports remaining issues, fix them manually and re-run.

## Commits

daml requires a DCO sign-off — always commit with `-s`:

```bash
git add <files>
git commit -s -m "your message here"
```

Before committing, verify git identity is configured — if not, stop and tell the user:

```bash
git config user.name && git config user.email || echo "git identity not set"
```

`setup-clones.sh` writes `user.name`/`user.email` into each clone when `GIT_USER_NAME` /
`GIT_USER_EMAIL` are exported. If identity is missing, the user must set it (`git config user.name` /
`git config user.email`) in the clone before you can commit.

## Git, PRs, and CI

Git works normally. Push and open PRs against the **daml upstream** (`digital-asset/daml`) — not
sbx-templates. Each clone is on its own branch with no tracking branch set until the first push; use
`-u` on the first push:

```bash
git push -u origin <branch>
```

CI runs on Azure Pipelines when the PR is opened. By default not every test runs on every commit;
adding a `run-all-tests: true` trailer to the commit message triggers the full set (see `sdk/build.sh`).
Don't fabricate other CI tags — follow what the PR template / repo docs say.

## If git commands fail

Each directory here is a standalone git clone. Check the basics:

```bash
git status
git remote -v   # origin should point at github.com/digital-asset/daml
```

**Do not attempt to re-clone or manipulate git yourself.** If the clone is broken beyond repair, tell
the user to remove and recreate it from the sbx-templates directory on their HOST:

```bash
export REPO_URL=https://github.com/digital-asset/daml
export SUBDIR=sdk
./scripts/remove-clones.sh <branch>
./scripts/setup-clones.sh <branch>
```

## If nix or bazel commands fail

Check whether the nix daemon socket is present:

```bash
[ -S /nix/var/nix/daemon-socket/socket ] && echo "daemon running" || echo "daemon missing"
```

In a template sandbox the daemon starts automatically at boot — if the socket is missing, something
went wrong at startup (logs at `/tmp/nix-daemon.log` or `/var/log/nix-daemon.log`). In a plain
sandbox (no template), re-run `scripts/bootstrap-nix.sh` from the sbx-templates directory to reinstall
nix and restart the daemon, then re-run `eval "$(dev-env/bin/dade assist)"` so the tool wrappers rebuild.
