# sbx-templates

Run nix-based projects inside [`sbx`](https://docs.docker.com/ai/sandboxes/) sandboxes without
re-downloading the toolchain on every fresh sandbox — plus a small multi-agent feature-clone layout.

The catch with a plain sandbox: `/nix` is wiped on rebuild, and a real dev shell (e.g. splice's ~19G)
doesn't fit the fixed 20G writable overlay (there's no disk-size flag). The fix is a **template
image** with the toolchain baked into **read-only layers**, which don't count against the overlay —
so a new sandbox boots with nix already present and nothing to download.

## Templates

Built by `scripts/build-template.sh`:

| Template | What it is |
|----------|-----------|
| `nix-direnv-sandbox` | Generic: the claude-code base + Nix, flakes, `direnv`/`nix-direnv`. The nix daemon **auto-starts at boot**, so nix + direnv just work. Point it at any nix repo. |
| `splice-ready` | `nix-direnv-sandbox` + [`canton-network/splice`](https://github.com/canton-network/splice)'s pinned **OSS** dev shell baked into `/nix`. |
| `daml-ready` | `nix-direnv-sandbox` + [`digital-asset/daml`](https://github.com/digital-asset/daml)'s pinned **nix dev-env** baked into `/nix`, plus a primed **bazel repository cache**. daml uses nix *and* bazel and its dev env lives under `sdk/`. |

**Step 1 — build the template.** The script only needs Docker, so run it wherever Docker is available:

```bash
# on the HOST (Docker Desktop) — simplest, no sandbox needed:
./scripts/build-template.sh base      # -> workspace/nix-direnv-sandbox.tar  (~900M)
./scripts/build-template.sh splice    # -> workspace/splice-ready.tar        (~8.5G; builds base first)
./scripts/build-template.sh daml      # -> workspace/daml-ready.tar          (large; builds base first)

# alternatively, inside any plain sbx sandbox with this repo as its workspace:
./scripts/build-template.sh splice
```

> **daml is heavy.** Building `daml-ready` clones daml, realizes its full nix toolchain
> (GHC/JDK/Scala/Node/…), and runs `bazel fetch //...` to prime the repository cache — gigabytes and
> a long build, needing broad network. **Build it on your host** (open network, ample disk). Narrow
> the fetch with `BAZEL_FETCH_TARGETS=//some/package/...` if `//...` is more than you need.

When building inside a sandbox, Docker build containers route through the sbx proxy, so you need the
domains allowed first (one-time on the host; see [`NETWORK-ALLOWS.md`](NETWORK-ALLOWS.md)):

```bash
sbx policy allow network install.determinate.systems,cache.nixos.org,nixos.org,releases.nixos.org,channels.nixos.org,repo1.maven.org
# for splice
sbx policy allow network www.canton.io,get.digitalasset.com,get.pulumi.com,api.pulumi.com
# for daml (nix + bazel caches; the bazel fetch needs broad egress — see NETWORK-ALLOWS.md)
sbx policy allow network nix-cache.da-ext.net,bazel-cache.da-ext.net,www.scala-lang.org,registry.npmjs.org,registry.yarnpkg.com,proxy.golang.org,sum.golang.org
```

> **Authenticate to GitHub before building.** The build resolves nixpkgs from
> `github:NixOS/nixpkgs/...` (nix hits the **GitHub API** to resolve a `github:` flake ref) and clones
> splice/daml from `github.com`. Unauthenticated, these go against GitHub's low anonymous rate limit
> and the **Determinate Nix install step fails** with:
> `process "/bin/bash … install.determinate.systems/nix …" did not complete successfully: exit code: 1`.
> So **before running `build-template.sh` on the host**, make sure you're logged in:
> ```bash
> gh auth login            # or: export GITHUB_TOKEN=<a token with public-repo / read access>
> gh auth status           # confirm
> ```

**Step 2 — load the template on the HOST (one-time):**

```bash
sbx template load workspace/<image>.tar
sbx template list   # confirm it's there
```

The template is now available to any sandbox you create — you don't rebuild or reload it per project
or per feature clone. See [`docs/templates.md`](docs/templates.md) for how it works and how to refresh.

## Sharing with colleagues

The templates aren't hosted anywhere — everyone builds their own from this repo. It's a couple of
setup minutes and avoids two headaches: hosting big images, and cross-arch (you build natively for
your own machine, arm64 or x86). Clone the repo, allow the build domains on your host
(`NETWORK-ALLOWS.md`), then `build-template.sh` → `sbx template load` → `sbx run -t` as above.

`splice-ready` holds **no secrets** (it's the OSS dev shell), but it *bundles* third-party binaries,
so building your own keeps you clear of any redistribution-licensing question. `nix-direnv-sandbox`
bundles only nix + direnv and is safe to push to a registry if you ever want to.

## Working modes

Three ways to run agents. Pick based on your needs:

| | sbx clone mode | Host clone mode | (old worktree mode) |
|---|---|---|---|
| **Clone lives** | Inside container | Host filesystem | Host filesystem |
| **Host visibility** | None — fetch or push to see changes | Live — git tooling and editors see changes immediately | Live |
| **`.direnv` attack surface** | None | Sandbox can write `.direnv/`; host direnv hook could load it | Same |
| **Persists across sandbox restart** | No — push first | Yes | Yes |
| **Setup** | None beyond the template | `setup-clones.sh` + `run-clone.sh` | (deprecated) |
| **Push to GitHub** | Works | Works | Works |

---

## sbx clone mode (safest)

No setup beyond the loaded template. Run from your existing splice checkout on the host.
`sbx run --clone` clones the repo from the current directory into the container — the host
filesystem is never mounted. Changes stay in the container until the agent pushes to GitHub.

```bash
cd /path/to/your/splice
sbx run --clone -t splice-ready --name my-feature claude
```

Splice build/env guidance (use `direnv exec .`, never bare `nix develop`, the `SBT_OPTS` heap, etc.)
is baked into the template's user memory at `~/.claude/CLAUDE.md`, so the agent picks it up here even
though the cloned splice repo carries no project `CLAUDE.md`. It lives in `docker/splice-ready.Dockerfile`
— edit it there and rebuild `splice-ready` to change it (the splice repo is never modified).

> Heads-up if you're used to "`direnv allow` and go": run build/test commands as
> `direnv exec . bash -c '…'`, not a bare `sbt …`. The agent's shells are non-interactive and one-shot
> (no direnv prompt hook), so `direnv allow` only *trusts* the `.envrc` — it doesn't keep the env
> loaded, and each command must re-apply it via `direnv exec .` or it runs without `SBT_OPTS`/`SPLICE_ROOT`/PATH.

No `REPO_URL`, no scripts. Each additional agent is another `sbx run --clone` with a different name.

**Working many features at once.** For a per-feature loop, `scripts/sbx-clone.sh <feature>` creates
(or resumes) one named `--clone` sandbox per feature and makes the `<feature>` branch inside the
clone; `scripts/fetch-sandbox.sh <feature> [<branch>]` pulls that sandbox's commits back to the host
via its `sandbox-<name>` remote. Park idle ones with `sbx stop`, remove merged ones with `sbx rm`.

**Inspecting logs.** In clone mode the clone (and its `log/`, `.build-logs/`) lives inside the
container, not on the host. The simplest way to read it is to open an interactive shell in the
sandbox and use `lnav` there — it's baked into the template's `agent` profile, so it's on PATH in
every sandbox without activating any dev shell:

```bash
sbx exec -it my-feature bash       # -it is essential — lnav is a full-screen TUI and needs a TTY
# inside:
lnav log/                          # canton.clog, toxi.log, …
lnav log/ .build-logs/             # build output too
```

**Pulling host edits into the clone.** In clone mode the container clone is isolated, so commits you
make on the host don't appear automatically. To pull them in, serve your host repos over the git
protocol and add a remote inside the sandbox (`host.docker.internal` resolves to the host).

On the HOST — serve everything under a base path, read-only:

```bash
git daemon --reuseaddr --export-all --base-path="$HOME/src" "$HOME/src"
# base-path ~/src exposes ~/src/splice as git://<host>/splice, ~/src/foo as git://<host>/foo, …
```

Inside the SANDBOX — add it once, then fetch whenever you want the latest host commits:

```bash
git remote add local git://host.docker.internal/splice
git fetch local && git merge local/<branch>     # or: git checkout -b <x> local/<branch>
```

Now edit on the host, commit, and re-run `git fetch local` in the sandbox to bring the changes over.

Caveats:
- **Port 9418** (git protocol). If the sandbox can't reach it, allow it on the host:
  `sbx policy allow network host.docker.internal:9418` (the policy is keyed by the port).
- **Committed objects only** — uncommitted working-tree edits on the host won't transfer; commit
  first (a throwaway commit you later amend/reset is fine).
- `--export-all` serves *any* repo under the base path with **no auth** (read-only) while it runs,
  and the daemon binds all interfaces by default (`--listen=127.0.0.1` to restrict). Start it only
  when you need it and stop it (Ctrl-C) when done.
- **Simpler built-in alternative:** in clone mode the host repo is already bind-mounted read-only at
  `/run/sandbox/source`, so for a one-off you can skip the daemon and just
  `git fetch /run/sandbox/source` from inside the sandbox.

For **daml**, use the `daml-ready` template. daml's dev env lives in the `sdk/` subdir, but its
**`.git` is at the repo root** (the parent of `sdk/`), and `git`/`build.sh`/commits need it. `sbx`
mounts *only the path you point it at*, so the rule is: **make the repo root the workspace, and work
in `sdk/`** — never point `sbx` straight at `sdk/`, or the parent `.git` is left outside the sandbox.

In clone mode the clone already contains `.git`, so naming `sdk` is fine:

```bash
cd /path/to/your/daml
sbx run --clone -t daml-ready --name my-feature claude sdk
```

For a **plain (non-clone) host mount**, point `sbx` at the repo **root** and `cd sdk` inside — *not*
at `sdk/` (that would leave `.git` unmounted: `fatal: not a git repository`):

```bash
sbx run -t daml-ready --name my-feature claude /path/to/your/daml   # mount the ROOT
# then inside the sandbox (no direnv needed — the dev-env wrappers self-bootstrap):
#   cd sdk && eval "$(dev-env/bin/dade assist)" && daml-bazel-prepare && bazel build //...
```

When done — if the agent pushed its branch, just remove the sandbox:

```bash
sbx rm my-feature
```

If the agent didn't push, retrieve commits before removing — the sandbox runs a git daemon while up:

```bash
# from your splice directory on the HOST:
git fetch sandbox-my-feature
git log HEAD..FETCH_HEAD --oneline
git merge FETCH_HEAD        # or cherry-pick
sbx rm my-feature
```

---

## Host clone mode

Each feature gets a standalone git clone on your host filesystem, bind-mounted into the sandbox.
Changes are visible live in your IDE and git tooling. The clone persists if the sandbox is removed.

```bash
export REPO_URL=https://github.com/canton-network/splice
./scripts/setup-clones.sh my-feature    # clones splice into workspace/splice-clones/my-feature/
./scripts/run-clone.sh my-feature       # launches or resumes the sandbox
```

Open `workspace/splice-clones/my-feature` in your git client as a normal repository.

**Reading build output live from the host.** Because the clone is bind-mounted, anything the
sandbox writes under it is on your host disk in real time. The splice `CLAUDE.md` tells the agent to
tee every build/test/lint command into `.build-logs/<name>.log` (line-buffered, so it streams), so
you can watch a compile from the host without attaching to the sandbox:

```bash
tail -f workspace/splice-clones/my-feature/.build-logs/compile.log
```

`setup-clones.sh` pre-creates `.build-logs/` and git-excludes it, so it never appears in commits.

For **daml**, set the template, repo, and the `sdk/` subdir; the scripts handle the rest:

```bash
export REPO_URL=https://github.com/digital-asset/daml
export TEMPLATE=daml-ready
export SUBDIR=sdk                        # daml's .envrc lives under sdk/
./scripts/setup-clones.sh my-feature     # clones daml into workspace/daml-clones/my-feature/
./scripts/run-clone.sh my-feature        # opens the agent in workspace/daml-clones/my-feature/sdk
```

---

## Multi-agent feature clones

> **Security note:** this mode bind-mounts the host filesystem into the sandbox. A compromised
> build (malicious dependency, not just the repo itself) could write to `.direnv/` in a mounted
> directory. If your host shell has the direnv hook active and you `cd` into that path, the tampered
> cache executes on your machine. Use clone mode if that risk is unacceptable.

With the template loaded, you can run several Claude agents in parallel — one per feature — without
building anything new. The three pieces are:

- **Template** (`splice-ready`) — the shared image, loaded once above. All sandboxes use it.
- **Sandboxes** — lightweight running environments, one per feature. Created with `run-clone.sh`, resumed by name.
- **Feature clones** — standalone git clones, one per feature branch. Normal git repos — open them directly in your IDE or git client.

**Step 1 — create the feature clones on the HOST:**

```bash
# from the sbx-templates directory:
export REPO_URL=https://github.com/canton-network/splice
export GIT_USER_NAME="Your Name"
export GIT_USER_EMAIL="you@example.com"
./scripts/setup-clones.sh feat-a feat-b
# clones splice into workspace/splice/, creates workspace/splice-clones/feat-a and feat-b
```

`GIT_USER_NAME` and `GIT_USER_EMAIL` are written into each clone's git config so that
`git commit -s` inside the sandbox produces a correct DCO signoff automatically.

Each clone is a normal branch of your project — `git push`, PRs, and remotes all go to the right
place with no awareness of `sbx-templates`. To add more clones later, just pass the new names:

```bash
./scripts/setup-clones.sh feat-c
```

The script is safe to re-run: existing clones are skipped.

To fix git identity on an already-created clone:

```bash
git -C workspace/splice-clones/<branch> config user.name "Your Name"
git -C workspace/splice-clones/<branch> config user.email "you@example.com"
```

> `workspace/splice/` is a reference clone used to speed up feature clones — it's not a hard
> dependency. Feature clones are fully independent after creation (`--dissociate`). You can safely
> delete `workspace/splice/` to reclaim disk; the next `setup-clones.sh` run will re-clone it.

**Step 2 — launch a sandbox per feature clone from the HOST:**

Export `REPO_URL` once, then use the script for both first launch and resume — it detects which
is needed automatically. Run from the `sbx-templates` directory so `sbx` mounts the full tree.

```bash
export REPO_URL=https://github.com/canton-network/splice
./scripts/run-clone.sh feat-a
./scripts/run-clone.sh feat-b
```

**Step 3 — remove a feature clone when the branch is merged (from the HOST):**

```bash
export REPO_URL=https://github.com/canton-network/splice
sbx rm feat-a
./scripts/remove-clones.sh feat-a
```

The script warns if the branch has unmerged commits, removes the clone directory, and deletes
the local branch from `workspace/splice/`. It does **not** delete the remote branch or close the
PR — do that on GitHub as normal.

## Plain sandbox (no template)

If you don't want a template, `scripts/bootstrap-nix.sh` installs the same toolchain into a running
sandbox (idempotent — re-run after a rebuild). It's the building block the `nix-direnv-sandbox`
image bakes in; the template just makes it persistent.

```bash
./scripts/bootstrap-nix.sh
bash -lc 'nix --version && direnv --version'
```

## What survives a sandbox rebuild

| Thing | Survives? |
|-------|-----------|
| This repo (host-mounted / on GitHub) | ✅ |
| `/nix`, the daemon, `~/.config` | ❌ — boot from a **template**, or re-run `bootstrap-nix.sh` |

## Notes

- **No systemd** — the nix daemon is started manually (templates do it at boot; the script does it on run).
- **`flakehub.com` is firewalled** — packages come from `github:NixOS/nixpkgs/...` + `cache.nixos.org`.
- **Push from inside the sandbox** — git auth is injected at the network layer (no `gh auth login`).
- **Building needs GitHub auth on the host** — `build-template.sh` resolves `github:NixOS/nixpkgs/...`
  via the GitHub API and clones the repos from `github.com`; run `gh auth login` (or export
  `GITHUB_TOKEN`) first, or anonymous rate limits fail the Determinate Nix step. See Step 1 above.
- **The sandbox is a containment boundary — keep secrets out of it.** It can't reach anything on the
  host outside the mounted dir, and the network is firewalled. Any secret you put inside (a mounted
  `.envrc.private`, an injected env var, etc.) becomes a capability the agent can read and use — it's
  not hidden from it. So don't expose what a task doesn't need. When a capability *is* needed, prefer
  network-layer injection (like the git-auth proxy above), which lets the sandbox use the credential
  without ever seeing its value.
- **`direnv` and `.envrc`/`.envrc.private` are an exposure path.** When the dev shell runs `direnv
  allow`, direnv evaluates these files inside the sandbox and exports whatever they set into the
  agent's environment. If a mounted `.envrc` (or a `.envrc.private` it sources) contains tokens,
  registry creds, or anything private, you've handed all of it to the agent the moment the shell
  loads. Treat anything reachable from the project's direnv files as visible to the sandbox, and keep
  private values out of files that get mounted in.
