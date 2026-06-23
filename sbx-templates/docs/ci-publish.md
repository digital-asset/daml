# Publishing the templates from CI (registry + multi-arch)

Goal: CI builds the template images and **pushes them to a container registry**, so a developer (or
another CI job) starts a sandbox with `sbx run -t <registry>/<name>:latest` and the image is pulled on
demand — no local `build-template.sh` + `sbx template load`.

## They're just OCI images — so this is the native model

The templates are plain Dockerfiles producing standard OCI images; the base is itself
`FROM docker.io/docker/sandbox-templates:claude-code-docker` (a registry image). The local tar +
`sbx template load` flow is only a workaround to dodge hosting big images and cross-arch; registry
hosting is the natural path.

**Bonus for CI:** when you `--push` to a real registry, the `FROM` chain resolves by *pulling* the
base — so CI does **not** need the local-registry dance `build-template.sh` uses for OCI-tar export.
Build the base first, then build the daml images with `--build-arg BASE=<registry>/nix-direnv-sandbox:<tag>`.

## 3 host types → 2 image architectures (the "3 variants" question)

We have three developer host types, but sbx sandboxes are **Linux containers regardless of host OS**
(macOS runs them inside Docker Desktop's Linux VM — there is **no macOS/darwin image**). So they map
to only **two** OCI platforms:

| Developer host | Container arch sbx runs | OCI platform to publish |
|---|---|---|
| Linux x86_64 | x86_64 linux | `linux/amd64` |
| Intel Mac (x86_64) | x86_64 linux (Docker Desktop VM) | `linux/amd64` |
| Apple Silicon Mac (arm64) | arm64 linux (Docker Desktop VM) | `linux/arm64` |

**Two image builds (`linux/amd64` + `linux/arm64`) cover all three host types** — Linux-x86 and
Intel-Mac share the amd64 image. Publish each image as a single **multi-arch manifest list**
(`<name>:latest`) and Docker/sbx auto-selects the matching arch. (Even if Intel Mac is gone, amd64
remains — it's also the Linux-x86 arch — so you never drop below two.)

## The daml caching reality — amd64 is the only *warm* variant

Multi-arch is easy for the light images but **not for `daml-prebuilt`**, because daml's caching infra
is x86-linux: the nix binary cache `nix-cache.da-ext.net` and the bazel remote cache
`bazel-cache.da-ext.net/202405/**ubuntu**` are amd64. `daml-prebuilt`'s value is the baked bazel
**disk cache**, which is architecture-specific and is populated by backfilling from that x86 remote
cache. Per image:

| Image | `linux/amd64` | `linux/arm64` |
|---|---|---|
| `nix-direnv-sandbox` (generic nix+direnv) | ✅ easy | ✅ easy (`cache.nixos.org` serves aarch64-linux) |
| `daml-ready` (nix dev-env + repo cache) | ✅ | ⚠️ feasible, but some dev-env nix may build from source (da-ext is x86) → slower |
| `daml-prebuilt` (baked bazel build cache) | ✅ **warm** (backfills from the x86 remote cache) | ❌ **no aarch64-linux remote cache to backfill** → a full from-source arm64 build, slow & possibly toolchain gaps |

It doesn't stop at build time: an Apple-Silicon dev on an arm64 sandbox **can't use daml's x86 remote
cache at runtime either**, so even `daml-ready` builds are uncached for them. That's a property of
DA's x86-only infra, not of the templates.

**Recommendation**
- Publish `nix-direnv-sandbox` (and `daml-ready`) **multi-arch** (amd64 + arm64).
- Publish `daml-prebuilt` **amd64-only** as the supported, warm artifact (covers Linux-x86 +
  Intel-Mac directly).
- Apple-Silicon devs who need `daml-prebuilt`, two options (both with caveats):
  1. **Run the amd64 image emulated** (`--platform linux/amd64`, Docker's qemu). The cache is warm so
     little *compilation* happens, but the baked outputs are x86 binaries (damlc, …) → run emulated,
     i.e. slower at runtime. Fine for light iteration, painful for heavy runs.
  2. **Build an arm64 `daml-prebuilt` from source** on a self-hosted arm runner — hours per refresh,
     no remote cache, and you must verify daml's nix/bazel toolchains build on aarch64-linux. Only
     worth it with several heavy arm users + arm CI capacity.
- The real long-term fix is DA standing up an **aarch64-linux** nix + bazel cache; until then arm64
  daml is second-class regardless of templating.

## Registry & tags

- **Registry:** pick one reachable by CI *and* devs — GHCR (`ghcr.io/<org>`), internal Artifactory, or
  GCP Artifact Registry. The workflow defaults to GHCR.
- **Tags:** `:latest` (moving) + an **immutable** tag. For `daml-prebuilt` use the baked daml SHA
  (`:daml-<sha>`, also in `/etc/daml-prebuilt.ref`) so a sandbox traces to an exact daml commit; for
  the light images, the sbx-templates repo SHA.

## Pipeline design — the considerations that shape the YAML

- **Native per-arch runners, not QEMU cross-build.** Emulating a daml build (or even daml-ready's nix
  realize) under QEMU is impractically slow/fragile. Use an amd64 runner and an arm64 runner; each
  builds its own arch and pushes **by digest**; a final step assembles the **manifest list** with
  `docker buildx imagetools create`.
- **Disk.** `daml-prebuilt` is tens of GB plus a huge transient output base. **GitHub-hosted runners
  (~14 GB) cannot build it** — use a **self-hosted / large runner with ≥100 GB free**. Light images
  may fit hosted runners (base ~900 MB; daml-ready bigger — watch it).
- **Network.** The daml builds must reach `*.da-ext.net` (nix + bazel caches). If those are inside
  DA's network, the runner must be **self-hosted inside that network** — public runners likely can't
  reach da-ext. (Public runners reach cache.nixos.org/github fine.)
- **GitHub rate limit.** nix resolves `github:NixOS/nixpkgs/...` via the GitHub API; in CI pass the
  job's token to nix (`NIX_CONFIG="access-tokens = github.com=<token>"`) to avoid the 60/hr anon cap.
  (The Dockerfiles don't wire a token today — see the workflow's commented `secret`/`build-arg` hook
  if you hit limits.)
- **FROM-base from the registry.** Build+push the base first; build daml images with
  `--build-arg BASE=<registry>/nix-direnv-sandbox:<tag>`.
- **Refresh cadence.** `daml-prebuilt` tracks main → run it on a **schedule** (e.g. nightly),
  resolving `DAML_REF` to main's HEAD. Rebuild the light images only when their Dockerfiles change.

## How sbx consumes a published image (verify on your host)

`sbx run` is host-side, so confirm the exact syntax with `sbx run --help` / `sbx template --help` /
docs.docker.com/ai/sandboxes. Most likely one of:
- `sbx run --clone -t <registry>/daml-prebuilt:latest … claude .` (direct pull; `--clone` path is the repo root), or
- `sbx template load <registry>/daml-prebuilt:latest` once (caches locally), then `sbx run -t daml-prebuilt …`.

Either way Docker/sbx selects the host-matching arch from the manifest list.

## Summary

- 3 host types → **2 arches**: amd64 (Linux-x86 + Intel-Mac) and arm64 (Apple Silicon).
- Light images: **multi-arch**. `daml-prebuilt`: **amd64 supported/warm**; arm64 only via emulation or
  an expensive from-source self-hosted build.
- CI: native per-arch runners + manifest list; **self-hosted DA runner** with big disk + da-ext access
  for the daml jobs; a token for the flake rate limit; schedule the `daml-prebuilt` refresh.

A reference GitHub Actions workflow implementing the supported path lives at
[`.github/workflows/publish-templates.yml`](../.github/workflows/publish-templates.yml) (in the
standalone sbx-templates repo it sits at `.github/workflows/`). It is a reviewed starting point — fill
in your runner labels, registry, da-ext reachability, and the verified `sbx` pull syntax; it can't be
CI-tested from here.
