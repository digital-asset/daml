# How the templates work

A sandbox can't keep a large nix store: `/nix` is wiped on rebuild, and a big dev shell (splice's is
~19G) doesn't fit the fixed 20G **writable** overlay. There's no disk-size flag, the nested-docker
disk isn't ours to use, and the host bind mount is case-insensitive (so it can't hold a nix store).

The fix is to bake the store into **read-only image layers**. Those are shared and don't count
against the writable overlay, so the store is present at boot regardless of its size, with nothing to
download. A sandbox boots from such an image with `sbx run -t <image>`.

## The layers

- **`nix-direnv-sandbox`** (`docker/nix-direnv.Dockerfile`) — the claude-code base + Determinate Nix,
  flakes, and `direnv`/`nix-direnv` (installed into the `agent` profile). Generic and free of any
  project content. Small (~900M).
- **`splice-ready`** (`docker/splice-ready.Dockerfile`) — `FROM nix-direnv-sandbox`, adding only
  splice's closure, realized with the exact command splice's `.envrc` uses
  (`nix print-dev-env --profile … "path:nix#oss"` at a pinned commit), so it's identical to a normal
  splice OSS build. The `--profile` roots the closure as a gc-root so it stays baked.
- **`daml-ready`** (`docker/daml-ready.Dockerfile`) — `FROM nix-direnv-sandbox`, adding daml's pinned
  nix dev-env **and** a primed bazel repository cache. See the daml section below.

All boot as `USER agent` (root would break the claude CLI), with `/nix` from read-only layers.

## The `daml-ready` layer (nix + bazel)

daml is harder than splice because it uses **nix and bazel**, and its dev env lives under the repo's
`sdk/` subdirectory. `sdk/.envrc` does `use nix` (realizes `sdk/shell.nix`) **and**
`eval "$(dev-env/bin/dade assist)"` — the "dade" system, whose tool wrappers in `sdk/dev-env/bin/*`
lazily run `nix-build sdk/nix/default.nix -A tools.<name>` on first use.

First it configures nix with **daml's own binary cache** (`nix-cache.da-ext.net`, from
`sdk/dev-env/etc/nix.conf`) and **disables the build sandbox**. Both matter: daml's pinned nixpkgs is
old (EOL python3.9) so many dev-env packages aren't in `cache.nixos.org`; and the few that build from
source run nixpkgs `checkPhase`s with localhost-networking tests (e.g. sphinx's `test_invalid_ssl`)
that fail under the restricted nix sandbox but pass unsandboxed. Disabling the sandbox keeps the
canonical store-path hashes (so runtime reuses them) — it only relaxes the build environment.

Then it bakes three things into the store at the pinned commit, each rooted so `nix store gc` can't
reclaim it:

1. **The dev-env tools** (GHC, JDK17, Scala, Node, Python, the `bazel_7` wrapper, …) — each
   `tools.<name>` from `sdk/nix/default.nix` is realized **individually and failure-tolerantly**:
   daml builds these lazily, so a few rarely-used tools may neither be cached nor build cleanly; those
   are skipped (they build lazily at runtime if ever invoked) rather than sinking the whole image. The
   essential toolchain is rooted here, so `dade assist`'s per-tool `nix-build` is an instant hit at
   runtime.
2. **dade's nixpkgs snapshot** — pre-created in a baked, overlay-writable `DADE_VAR_DIR`
   (`/opt/daml-dade-var`) along with its hash file, so `dade assist` finds a matching snapshot and
   **skips its own realise**. That realise is broken under Determinate Nix:
   `nix-instantiate --eval nixpkgs.nix -A path` prints the fetchTarball *URL* instead of a store path,
   so we root the nixpkgs source directly via `toString` instead. (This is why the template is pinned
   to `DAML_REF`: dade keys the snapshot on `nix-hash nix/nixpkgs.nix`, so the clone should sit at /
   near the baked commit — otherwise dade falls back to its own, broken, realise.)
3. **The `sdk/shell.nix` closure** (the `use nix` half), via `nix print-dev-env --profile …`.

**Bazel caching.** bazel pulls gigabytes of *external* deps (maven, stackage, npm/yarn, go,
`http_archive`s) that aren't nix-sourced. The Dockerfile primes them with
`bazel fetch ${BAZEL_FETCH_TARGETS}` into a read-only baked `--repository_cache`
(`/opt/daml-bazel-cache/repo`), wired at runtime through a baked `~agent/.bazelrc` (a home bazelrc is
read after the workspace `.bazelrc`, so its `--repository_cache` wins over daml's default
`.bazel-cache/repo`). The baked cache is a **read-only seed**: pre-baked deps are hits; a genuinely
new dep downloads fresh (bazel warns it can't write the read-only cache, then proceeds).

**What is *not* baked: built action outputs.** bazel must write its output base at runtime, so it
can't be a read-only layer. By default it lands in `~/.cache/bazel` on the **fixed 20G overlay**, and
a daml build overflows it. The image bakes a helper, **`daml-bazel-prepare`**, that the runtime
CLAUDE.md runs once before building: it writes `sdk/.bazelrc.local` to (1) relocate the output base to
`sdk/.bazel-cache/output` on the **host-mounted** workspace disk (roomy — works in plain-host-mount or
host-clone mode, not pure `--clone`), and (2) set `--remote_download_outputs=toplevel`
(build-without-the-bytes) so intermediate outputs stream from daml's **remote cache**
(`bazel-cache.da-ext.net`, pull-only, when allowed in the network policy) rather than filling local
disk. Even so, scope builds to specific targets rather than `//...`. The host mount is virtiofs
(slow); `daml-bazel-prepare --vdc` instead targets the sandbox's **local** 50G `vdc` disk (mounted at
`/var/lib/docker`, the near-empty nested-Docker disk) for fast + roomy builds — at the cost of being
sandbox-lifetime-only and shared with Docker. The helper rewrites a managed block in `.bazelrc.local`,
so switching modes (or re-running) is idempotent.

## Daemon auto-start

Multi-user Nix needs its daemon running to do real work. The base image installs a tiny entrypoint
wrapper (`ENTRYPOINT ["tini","--","/usr/local/bin/nix-daemon-entrypoint"]`) that starts the daemon
once at boot, then execs the real command — so nix and direnv work the moment the sandbox is up, with
no bootstrap step. (The per-command persistent env file can't start a daemon; hence the wrapper.)

## Security

Runtime processes run as non-root `agent`. No secrets or proxy CA are baked in. The root nix daemon
plus its sandboxed `nixbld` build users are the standard multi-user Nix model; the `agent` user's
passwordless sudo and docker-group membership are inherited from the claude-code base image, not
added here. The entrypoint is a root-owned script running a fixed command — it grants nothing beyond
what the base already allows. The sandbox itself is the isolation boundary.

## Building

`scripts/build-template.sh base | splice` builds in the nested Docker daemon and writes a tar to
`workspace/`. A few details that matter on the ~49G nested-docker disk:

- The large `splice` image is exported with `docker buildx --output type=oci,dest=<tar>` straight to
  the host mount, which avoids unpacking a second copy into the local image store (two copies of a
  19G store don't fit). `-t` is passed so the tar carries the image name and `sbx template load` tags
  it correctly.
- `splice` loads the base from `workspace/nix-direnv-sandbox.tar` rather than rebuilding it.
- The built image is reclaimed after export (the tar is the deliverable); `KEEP_IMAGE=1` keeps it.

## Refresh

Re-run `build-template.sh splice` (bump `SPLICE_REF`) when splice's `nix/flake.lock` or
`nix/*-sources.json` change. Re-run `build-template.sh daml` (bump `DAML_REF`) when daml's
`sdk/nix/src.json`, `sdk/nix/nixpkgs/`, or its bazel external deps change. Re-run `... base` only when
nixpkgs itself moves (direnv/nix-direnv update). On the host, `sbx template rm <image>` before
re-loading to replace a previous version cleanly.

## Why not just…?

The whole approach hinges on one fact: a sandbox's writable overlay is a **fixed 20G**, splice's dev
shell is **~19G**, and there's **no way to enlarge the disk** (no `sbx` flag, no `DOCKER_SANDBOXES_*`
knob, Docker Desktop's image size isn't the limiter). Given that, baking the store into read-only
layers is the standard way to fit a large prepopulated store into a constrained container. If that
disk constraint ever goes away, baking becomes optional (a faster start), not necessary.

- **…use a binary cache / substituter instead of baking?** That speeds up *downloads*; it doesn't
  solve *space or persistence*. The store still has to land somewhere writable and ≥19G — which the
  overlay isn't. (splice also ships no public binary cache; its caching is CI-only.)
- **…give the sandbox a bigger disk?** No mechanism exists to size the sandbox root disk.
- **…use `sbx template save`?** It stops the sandbox and snapshots only the rootfs, so a store that
  doesn't fit the overlay is captured empty. We build with Docker + `sbx template load` instead.
- **…mount nix from the host?** Tried and measured — not viable on Docker Desktop. The store
  filesystem itself can be made case-sensitive (a dedicated case-sensitive APFS volume), and
  `chown`/`chmod`/xattrs all pass through virtiofs. But nix needs three things virtiofs withholds:
  container-root **DAC override** (to delete its read-only store paths — GC/optimise fail with
  `EACCES`), faithful **uid** recording on build-user writes (multi-user builds are rejected as
  "suspicious ownership"), and **metadata coherence** after writes (the profile builder fails with
  `ENOENT` on a file it just created). The last has no nix-side workaround. Baking into read-only
  image layers sidesteps all of it: the store is a real, coherent Linux fs and is never mutated at
  runtime.
- **…single-user nix instead of the multi-user daemon?** A fair simplification — it would drop the
  daemon, sudo, and the entrypoint wrapper. We use multi-user because it matches the claude-code base
  and the security difference is nil (the base already grants `agent` passwordless sudo). Switching
  is a reasonable future change if the daemon machinery feels heavy.
- **…use an official splice dev image, if one exists?** If Canton/DA publishes one, prefer it for
  `splice-ready`. The generic `nix-direnv-sandbox` is useful either way.
