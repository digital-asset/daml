# daml-prebuilt — build & run

`daml-prebuilt` is the daml sandbox image with **both** the nix dev-env **and** a full
`bazel build //...` cache baked into read-only layers. A fresh sandbox starts warm (near-all cache
hits, offline) with almost all of the fixed 20 GB writable overlay free.

There are two operations: **build the image** (occasional, on a host/CI) and **launch a sandbox from
it** (per feature, with `--clone`).

---

## Part 1 — Build the image

Run on a machine with Docker and an open network (your **host** / Docker Desktop, or CI). The build
clones daml, realizes its nix toolchain, and runs a full `bazel build //...` — it pulls **gigabytes**
and wants **tens of GB of free disk**. Do it occasionally and refresh by bumping the ref.

### 1.1 One-time host prerequisites

**GitHub auth — only if you hit a rate-limit 403.** nix resolves `github:NixOS/nixpkgs/...` by
calling the GitHub API, which is capped at **60 requests/hour for unauthenticated IPs**. On a personal
host doing an occasional build you usually won't hit it, and **nothing here is private** (nixpkgs and
daml are public) — so no token is required to start. Only if the build fails with a GitHub
**403 / rate-limit** message, raise the ceiling on your own machine (or just wait an hour and retry):

```bash
gh auth login                       # interactive, on your host — no token to hand around
# or:  export GITHUB_TOKEN=<token>  # any token; it only lifts the rate limit
```

**Allow the build network domains** (host-level, default-deny firewall). `bazel-cache.da-ext.net` is
**mandatory** for daml-prebuilt — it's the remote cache whose downloads populate the baked disk cache:

```bash
# generic nix toolchain (the base image)
sbx policy allow network install.determinate.systems,cache.nixos.org,nixos.org,releases.nixos.org,channels.nixos.org,repo1.maven.org
# daml nix + bazel caches + language deps + runtime: da-ghc GHC submodule cloning
sbx policy allow network nix-cache.da-ext.net,bazel-cache.da-ext.net,www.scala-lang.org,registry.npmjs.org,registry.yarnpkg.com,proxy.golang.org,sum.golang.org,gitlab.haskell.org
```

> If the build is happening on a plain host with Docker Desktop (not inside an sbx sandbox), the
> `sbx policy` rules don't gate it — your normal host network does. The list above is for building
> *inside* a sandbox. Either way the same domains must be reachable.

### 1.2 Build

**Pin `DAML_REF` to the exact commit you'll work from.** The build bakes the cache from this daml
commit; pinning it (rather than floating on `main`) keeps the image reproducible and minimises drift
from the checkout you later `--clone`. Use the commit carrying these fixes on
`daml-prebuilt-cache-fixes` (push it first — see the note below; you can also pass the branch name and
`build-template.sh` resolves it):

```bash
cd sbx-templates
DAML_REF=d9ae8e010b5c5b2649ff87b9ff41f717d1053b02 ./scripts/build-template.sh daml-prebuilt
# -> workspace/daml-prebuilt.tar   (builds nix-direnv-sandbox first if missing)
```

> **The ref must be reachable on `$DAML_REPO`.** The build clones daml from GitHub
> (`https://github.com/digital-asset/daml` by default), so **push the branch/commit first** — a
> local-only commit fails the in-build `git fetch`. (The grpc_haskell source fix, bug 2, doesn't need
> to be in this ref: it reaches the sandbox via `--clone` in Part 2. The baked repo cache already holds
> the archive by content hash regardless.)

Useful overrides (env vars):

| Var | Default | Effect |
|-----|---------|--------|
| `DAML_REF` | **current tip of `main`** (resolved to a SHA at build time) | `DAML_REF=<full-sha>` to pin a specific commit (recommended — see above), or `DAML_REF=<branch>` to track another branch. Must be reachable on `$DAML_REPO`. |
| `BAZEL_BUILD_TARGETS` | `//...` | narrow to e.g. `//compiler/...` for a faster, smaller image |

**Watch the build log for this line:**

```
[daml-prebuilt] baked disk cache size: <N> bytes (floor: 2000000000)
```

The build **fails fast** if `<N>` is below the floor — that means the remote→disk cache write-through
didn't happen (check that `bazel-cache.da-ext.net` was reachable), rather than silently shipping an
empty cache.

### 1.3 Load it on the host (replaces the existing template)

This rebuilt image **replaces** the current `daml-prebuilt` template. Remove the old one first —
otherwise the load can silently keep the stale image under that name:

```bash
sbx template rm   daml-prebuilt                  # drop the old image (no-op if none is loaded yet)
sbx template load workspace/daml-prebuilt.tar    # load the freshly built replacement
sbx template list                                # confirm `daml-prebuilt` is present
```

The template is now available to any sandbox — you don't rebuild or reload it per feature.

### 1.4 Refresh later

Just re-run the build with `DAML_REF` pinned to the new commit (e.g.
`DAML_REF=<full-sha> ./scripts/build-template.sh daml-prebuilt`); omit it only if you deliberately want
the current tip of `main`. On the host, `sbx template rm daml-prebuilt` before re-loading to replace it
cleanly (see §1.3). Drift between the baked ref and a checkout isn't an error — bazel just rebuilds the
delta.

---

## Part 2 — Launch a sandbox with `--clone`

`--clone` clones your **entire daml worktree** (repo root, including `.git`) from the current
directory into the container. The clone lives **inside** the container — the host filesystem is never
mounted — and changes stay there until you push or fetch them out.

> **Why the repo root matters:** daml's dev-env is under `sdk/` but its `.git` is at the repo
> **root**. With `--clone` the clone already contains `.git`, so you point `claude` at the `sdk`
> subdir and everything (build *and* git) works.

### 2.1 Start it

Run this **on the host** (not inside an existing sandbox — check `echo $SANDBOX_VM_ID` is empty). The
positional path is **the directory `--clone` clones, and it must be a git repo root** (contain `.git`).
daml's `.git` is at the **repo root**, so pass the root (`.` from there) — **not** `sdk` (`sbx --clone`
errors with *"… /daml/sdk is not in a Git repository"* because `.git` isn't in `sdk/`):

**Create the sandbox, then attach** (two steps — `sbx create` clones the repo in and provisions it;
`sbx run` attaches). Launch from a checkout on the branch that carries the grpc_haskell fix (bug 2), so
the `--clone` inside the container has it:

```bash
cd /path/to/your/daml                                   # the repo ROOT (has .git)
git checkout daml-prebuilt-cache-fixes                  # commit d9ae8e010b5 — carries the bug-2 fix into the clone
sbx create --clone -t daml-prebuilt --name my-feature claude .   # create + clone the repo in
sbx run --name my-feature                               # attach to it
```

claude opens at the repo root; it `cd`s into `sdk/` to build (the baked `~/.claude/CLAUDE.md` says so).

> **One sandbox per workspace directory.** sbx identifies a sandbox by the directory you launch from.
> If that dir already has a sandbox, sbx resolves to it and **ignores `--name`/`--template`** (you'll
> see *"sandbox '<existing>' already exists; --name, --template can only be used when creating a new
> sandbox"*). To run a **second** sandbox (e.g. to test this template while another session is open on
> the same checkout), launch from a **separate clone**:
> ```bash
> git clone /path/to/your/daml ~/daml-test && cd ~/daml-test
> sbx create --clone -t daml-prebuilt --name daml-prebuilt-test claude .
> sbx run --name daml-prebuilt-test
> ```

Resume a stopped sandbox with `sbx run --name <name>`.

### 2.2 One-time network requirement

`gitlab.haskell.org` must be in the sandbox's network policy (covered by the `sbx policy allow`
block in §1.1). Bazel's `da-ghc` external repo (`github.com/digital-asset/ghc`) has GHC submodules
hosted on `gitlab.haskell.org` that must be cloned **once** when Bazel populates a fresh output
base (the main repo comes from the baked repo cache; only the submodule clone step needs the
network). Without this the build fails at analysis. This is a one-time fetch per output base —
subsequent builds use the locally-materialized external dir and run fully offline.

### 2.3 First build inside the sandbox

The baked `~/.claude/CLAUDE.md` already tells the agent what to do; the manual equivalent (claude
starts at the repo root, so `cd sdk` first):

```bash
cd sdk                                # daml's dev-env + bazel workspace live here
eval "$(dev-env/bin/dade assist)"     # put bazel/jdk/scala/… on PATH (once per shell)
daml-bazel-prepare --vdc              # see note below — REQUIRED in clone mode
bazel build //compiler/damlc:damlc    # or //... — should be near-all cache hits, offline
```

> **Use `daml-bazel-prepare --vdc` in clone mode.** The baked caches are read-only layers (free), but
> bazel's writable **output base** is not. In `--clone` mode the worktree sits on the 20 GB overlay,
> so the default (host-disk) relocation has nowhere roomy to go. `--vdc` puts the output base on the
> sandbox's local 50 GB disk (`/var/lib/docker`) — fast and roomy. (Trade-off: it's wiped if the
> sandbox is recreated, and shared with any nested Docker.)

**Verify the cache is live:** the first build should report almost all actions as cached with no
recompilation. If it recompiles heavily, either the checkout drifted far from the baked ref
(`cat /etc/daml-prebuilt.ref`) or the `--config=linux` it's keyed under got doubled or changed. The
nix `bazel` wrapper injects `--config=linux` exactly once — don't add another copy (in `~/.bazelrc` or
on the command line; a duplicate breaks protoc) and don't force a different `--config` (zero hits).

Runtime needs **no network** for builds **except** the one-time `da-ghc` submodule clone on a fresh
output base (see §2.2 above). After that first fetch, builds run fully offline from the baked caches.

> For a full post-launch verification checklist — what an agent should check and report inside the
> running sandbox — see [`daml-prebuilt-verify.md`](daml-prebuilt-verify.md).

### 2.4 Get your commits out

Commit inside the sandbox with a **DCO sign-off** (daml requires it):

```bash
git config user.name && git config user.email || echo "set git identity first"
git add <files> && git commit -s -m "your message"
```

Then either **push from inside the sandbox** (git auth is injected at the network layer — no
`gh auth login` needed inside):

```bash
git push -u origin <branch>
```

…or, if pushing fails with `could not read Username for 'https://github.com'`, the host needs a token
secret (run on the **host**, then retry the push):

```bash
sbx secret set my-feature github -t "$(gh auth token)"   # <name> from $SANDBOX_VM_ID inside the sandbox
```

…or pull the commits back to the host **without** GitHub — the sandbox serves a git daemon while up:

```bash
# on the HOST, from your daml checkout:
git fetch sandbox-my-feature
git log HEAD..FETCH_HEAD --oneline
git merge FETCH_HEAD        # or cherry-pick
```

### 2.5 Inspect / clean up

```bash
sbx exec -it my-feature bash     # open a shell in the sandbox (-it needed for TUIs like lnav)
sbx stop my-feature              # park an idle sandbox
sbx rm   my-feature              # remove it (push or fetch your commits FIRST — the clone is in-container)
```
