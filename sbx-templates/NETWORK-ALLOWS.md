# Network allowlist log

The sandbox is **default-deny** for outbound network. Each domain below was proposed by the agent
and allowed by the user on the **host** with `sbx policy allow network <domain>`. This is the record
of what was opened, why, and what fetches over it.

## Access scope — important

- `sbx policy allow network <domain>` opens **outbound connectivity** to that domain at the firewall.
  It is **domain-level**, not method-level — the rule itself does not restrict traffic to "read only".
- In practice everything here is **anonymous download of public artifacts** (installers, binary
  caches, source tarballs, plugin archives). The agent holds **no credentials** for any of these
  hosts — no API tokens, no logins, no push rights.
- Concrete proof it can't write: `git push` from the sandbox **failed** with
  `could not read Username for 'https://github.com'` because no GitHub token secret is configured.
  So even GitHub is effectively read-only from inside the sandbox.
- Write/publish access would only exist if you explicitly provisioned credentials
  (e.g. `sbx secret set <sandbox> github -t ...`, or an `ARTIFACTORY_PASSWORD`). None are set.

**So: yes — in practice these are all read-only fetches, because the agent has no credentials.**
Just note the read-only property comes from *absence of creds + public endpoints*, not from the
allow rule (which only governs reachability).

## Allowed

**For the `base` / generic nix toolchain** (the `nix-direnv-sandbox` build):

| Domain | What it fetches |
|--------|-----------------|
| `install.determinate.systems` | Determinate Nix installer script |
| `cache.nixos.org` | nix binary cache (prebuilt store paths) |
| `nixos.org`, `releases.nixos.org`, `channels.nixos.org` | nix site / release artifacts / channels |
| `repo1.maven.org` | Maven Central — sbt/coursier JVM deps |

**Additionally for `splice`** (the OSS dev shell, on top of the above):

| Domain | What it fetches |
|--------|-----------------|
| `www.canton.io` | Canton open-source tarball (`canton-open-source-*.tar.gz`) |
| `get.digitalasset.com` | dpm-sdk download |
| `get.pulumi.com` | base pulumi CLI + default provider plugin tarballs |
| `api.pulumi.com` | pulumi plugins hosted only here (gcp 9.16.0, postgresql, tls, vault) |

**Additionally for `daml`** (the nix dev-env + priming bazel's repository cache, on top of the base):

| Domain | What it fetches |
|--------|-----------------|
| `nix-cache.da-ext.net` | daml's **nix binary cache** (`sdk/dev-env/etc/nix.conf`) — prebuilt dev-env packages absent from cache.nixos.org for daml's pinned (EOL python3.9) nixpkgs. Without it the dev-env builds from source. |
| `bazel-cache.da-ext.net` | daml's **remote bazel cache** (pull-only) — prebuilt action outputs. Also wanted at *runtime* in the sandbox. |
| `www.scala-lang.org` | Scala 2.13.16 tarball pinned in `sdk/nix/nixpkgs.nix` |
| `registry.npmjs.org`, `registry.yarnpkg.com` | npm / yarn deps fetched by `bazel fetch` |
| `proxy.golang.org`, `sum.golang.org` | Go module deps + checksum db |
| `repo1.maven.org` | Maven Central — JVM deps (already allowed for the base) |

**Additionally at `daml-prebuilt` runtime** (needed for the first build per output base):

| Domain | What it fetches |
|--------|-----------------|
| `gitlab.haskell.org` | GHC submodules — `da-ghc` is a `git_repository` for `github.com/digital-asset/ghc` with `recursive_init_submodules = True`; GHC's submodules (haddock, hpc, hsc2hs, arcanist linter) live on `gitlab.haskell.org` and are cloned when Bazel first populates the `da-ghc` external dir on a fresh output base. The main repo comes from the baked repo cache; only the submodule clone step needs the network. This is a one-time cost per output base — subsequent builds use the locally-materialized external dir. |

> **The `daml` build's bazel-fetch step needs broad outbound egress** (the hosts above plus many
> github release assets). It downloads gigabytes of external deps to prime the repository cache. This
> is exactly why the intended flow is to **build `daml-ready` on your host**, where the network is
> open — not inside a default-deny sandbox. If you must build inside a sandbox, expect to allow more
> domains iteratively as `bazel fetch` reports 403s (read the blocked-response body for the host).

> **`daml-prebuilt` needs the same domains as `daml`, with `bazel-cache.da-ext.net` mandatory.** Its
> build runs a full `bazel build //...` (not just `fetch`), and relies on daml's remote bazel cache to
> turn most actions into downloads instead of recompiles. Those downloads are what populate the baked
> `--disk_cache` (combined-cache write-through), so if `bazel-cache.da-ext.net` is unreachable the
> build is both far slower and the size assertion may fail. Build it on the host/CI with the daml
> domains allowed. Note `bazel-cache.da-ext.net` is needed only at **build** time for `daml-prebuilt`
> (the cache is then baked) — unlike `daml-ready`, which wants it at runtime too.

```bash
# base / generic nix toolchain
sbx policy allow network install.determinate.systems,cache.nixos.org,nixos.org,releases.nixos.org,channels.nixos.org,repo1.maven.org
# additionally, for splice
sbx policy allow network www.canton.io,get.digitalasset.com,get.pulumi.com,api.pulumi.com
# additionally, for daml (nix + bazel caches are also wanted at runtime)
sbx policy allow network nix-cache.da-ext.net,bazel-cache.da-ext.net,www.scala-lang.org,registry.npmjs.org,registry.yarnpkg.com,proxy.golang.org,sum.golang.org
# additionally, for daml-prebuilt at runtime (one-time da-ghc submodule clone per output base)
sbx policy allow network gitlab.haskell.org
```

## Already reachable — no allow needed

`github.com`, `api.github.com`, `codeload.github.com`, `objects.githubusercontent.com` — used for
flake inputs (`nixpkgs`, `flake-utils`), the splice clone, and github-hosted pulumi plugins.

> **Reachable ≠ authenticated.** These hosts are allowed, but the build still makes GitHub requests
> **anonymously** unless you've authenticated. nix resolves `github:NixOS/nixpkgs/...` via the GitHub
> API, which has a low unauthenticated rate limit — exhausting it fails the build at the Determinate
> Nix install step. Run `gh auth login` (or export `GITHUB_TOKEN`) on the host **before**
> `build-template.sh`.

## Considered but intentionally NOT allowed (still blocked)

| Domain | Why proposed | Why NOT allowed |
|--------|--------------|-----------------|
| `flakehub.com` | Determinate's default registry for bare `nixpkgs#` | Avoided by using `github:NixOS/nixpkgs/...` refs |
| `digitalasset.jfrog.io` | Artifactory for **enterprise** Canton | OSS dev shell doesn't use it (would also need creds) |

_Verify current state anytime with `sbx policy ls` / `sbx policy log` on the host, or from inside the
sandbox by `curl`-ing a domain (a `403` body starting "Blocked by network policy" = still denied)._
