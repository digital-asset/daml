# daml-prebuilt sandbox

This sandbox is the [digital-asset/daml](https://github.com/digital-asset/daml) repo with daml's nix
dev-env **and a fully-baked bazel build cache** baked into read-only image layers. The dev environment
lives under the **`sdk/`** subdirectory — run all build/test commands from `sdk/`.

> **`git` says "not a git repository"?** daml's `.git` is at the **repo root** (the parent of `sdk/`).
> The sandbox must be launched with the **repo root** as the workspace (then `cd sdk`), not `sdk/`
> itself, or the parent `.git` is left unmounted.

## What's baked (and what that means for you)

- **Toolchain (nix dev-env):** present at boot, nothing to download.
- **Bazel external deps (`--repository_cache`):** baked read-only.
- **Bazel action outputs (`--disk_cache`):** baked read-only — a full `bazel build //...` was run at
  image-build time and its results are baked in. So your **first build is near-all cache hits and runs
  offline** — no recompiling the world, no network to daml's remote cache needed.

`~/.bazelrc` (baked) already points bazel at both caches and forces `--config=linux`. **Do not pass a
different `--config`** — the cache is keyed under `--config=linux`; a different config gets zero hits.

## Build (from `sdk/`)

```bash
eval "$(dev-env/bin/dade assist)"   # put bazel/jdk/scala/… on PATH — once per shell
daml-bazel-prepare                  # once per checkout — see "Disk" below
bazel build //compiler/damlc:damlc  # or //... — near-all cache hits the first time
```

Verify the cache is working: the first build should report almost all actions as cached with no
recompilation. If instead it recompiles heavily, the checkout has drifted from the baked ref
(`cat /etc/daml-prebuilt.ref`) — expected to rebuild only the delta.

## Disk — keep the writable overlay free (READ THIS)

The baked caches ride in read-only layers and are **free of the fixed 20 GB overlay**. But bazel's
**output base** (the materialized build tree) must be writable. By default it lands on the 20 GB
overlay and a daml build overflows it. **Run `daml-bazel-prepare` once** (from `sdk/`) before building
— it relocates the output base off the overlay and enables build-without-the-bytes:

```bash
daml-bazel-prepare          # output base -> sdk/.bazel-cache/output on the host-mounted disk (roomy, persistent, slower virtiofs)
daml-bazel-prepare --vdc    # output base -> /var/lib/docker on the local disk (fast; sandbox-lifetime; shared with Docker)
```

Build-without-the-bytes is safe here: it streams intermediates from the **local baked disk cache**,
which (unlike a remote cache) never evicts, so there are no missing-CAS failures. With the baked cache
+ relocated output base, a full `bazel build //...` is allowed; scope to a target when you want it
faster.

## Tests

`--test_output=errors` is the default. Scope tests to a target:

```bash
bazel test //daml-lf/data:data-test
```

If asked to run tests without a specific target, ask the user which target to run.

## Before a PR

```bash
./fmt.sh                  # scalafmt, hlint, java/yapf, prettier, copyright headers, …
```

Fix anything it reports, then re-run.

## Commits & PRs

daml requires a **DCO sign-off** — always commit with `-s`:

```bash
git config user.name && git config user.email || echo "git identity not set — tell the user"
git add <files> && git commit -s -m "your message"
```

Push and open PRs against **daml upstream** (`digital-asset/daml`). CI runs a reduced test set on PRs;
add a `run-all-tests: true` commit-message trailer to run the full suite (see `sdk/build.sh`). Don't
invent other trailers.
