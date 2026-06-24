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

`~/.bazelrc` (baked) already points bazel at both caches. `--config=linux` is injected by the nix
`bazel` wrapper (`dev-env/bin/bazel`), so it is **not** repeated in `~/.bazelrc`. **Do not add
`--config=linux` yourself, or pass a different `--config`** — a duplicate doubles every `:linux` flag
and breaks protoc; a different config is keyed differently and gets zero cache hits.

## Network — one domain needed for the first build

Bazel's `da-ghc` external repo has GHC submodules on `gitlab.haskell.org` that must be cloned once
when the output base is fresh. If the first build fails at analysis with a `403` from
`gitlab.haskell.org`, tell the user to run on the **host** and retry:

```bash
sbx policy allow network gitlab.haskell.org
```

This is a one-time fetch. After the `da-ghc` external dir is populated, subsequent builds run
offline from the baked caches.

## Build (from `sdk/`)

```bash
eval "$(dev-env/bin/dade assist)"   # put bazel/jdk/scala/… on PATH — once per shell
daml-bazel-prepare                  # once per session — configures cache paths
bazel build //compiler/damlc:damlc  # or //... — near-all cache hits the first time
```

`daml-bazel-prepare` auto-detects the daml-prebuilt environment and configures Bazel to use the
baked disk cache. It works from any workspace path — no need to `cd /opt/daml/sdk`.

Verify the cache is working: the first build should report almost all actions as cached with no
recompilation. If instead it recompiles heavily, the checkout has drifted from the baked ref
(`cat /etc/daml-prebuilt.ref`) — expected to rebuild only the delta.

## Disk — keep the writable overlay free (READ THIS)

The baked caches ride in read-only layers and are **free of the fixed 20 GB overlay**. But bazel's
**output base** (the materialized build tree) must be writable. **Run `daml-bazel-prepare` once**
(from `sdk/`) before building — it configures Bazel to use the baked output base for cache hits.

In daml-prebuilt sandboxes, `daml-bazel-prepare` automatically detects the baked environment and
configures `--output_base` to point to the exact baked directory. This ensures cache hits regardless
of where your workspace is mounted (e.g., `/home/<user>/daml/sdk` vs `/opt/daml/sdk`).

`daml-bazel-prepare` also configures `--remote_download_outputs=all` (full materialization), **not**
build-without-the-bytes: the baked disk cache doesn't hold every intermediate and daml's remote CAS
evicts, so BwoB would hit `CacheNotFoundException`. Outputs are materialized from the **local baked
disk cache** (offline). With the baked cache, a full `bazel build //...` is allowed; scope to a
target when you want it faster.

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
