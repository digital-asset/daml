# Splice sandbox environment (Host clone mode)

Shared splice build/env/test/commit guidance — `direnv exec . bash -c …`, the `USER=$(id -un)`
prefix, why not bare `nix develop`, `SBT_OPTS`, sbt project IDs (`apps/app` → `apps-app`),
integration-test pre-flight, `formatFix`/`lint`, CI tags, reading logs — lives in the template's
baked user memory at `~/.claude/CLAUDE.md` (source: `docker/splice-ready-claude.md`) and is loaded
here too. **This file only adds what's specific to Host clone mode** (the host-mounted clone).

## Always save build/test output to `.build-logs/` (host-visible, live)

Unlike sbx clone mode, this clone is bind-mounted over virtiofs, so the `.build-logs/` directory at
the clone root is mirrored to the HOST in real time. **Every build/test/lint command must tee its
output to `.build-logs/<name>.log`** so the user can `tail -f` (or `lnav`) it on the host without
attaching to the sandbox. Canonical form — three pieces matter:

```bash
USER=$(id -un) direnv exec . bash -c \
  'set -o pipefail; mkdir -p .build-logs; stdbuf -oL -eL sbt --batch compile 2>&1 | tee .build-logs/compile.log'
```

- `set -o pipefail` (inside `bash -c`) so the command's real exit code propagates — a bare
  `… | tee` always reports tee's success and would hide a failed build.
- `stdbuf -oL -eL` forces **line buffering**: piping to `tee` makes many tools switch to 4–8 KB
  block buffering, so the host `tail -f` lags in bursts instead of updating live. (sbt's JVM logger
  usually line-flushes anyway, but npm/eslint/daml/shell commands need this.) If `stdbuf` is somehow
  unavailable, drop it — the log is still captured, just less smoothly.
- `mkdir -p .build-logs` in the command itself — don't rely only on `setup-clones.sh`; existing
  clones predate it.

On the HOST the user reads it at `workspace/splice-clones/<branch>/.build-logs/compile.log`.

- **One log name per command** (`compile.log`, `test-<class>.log`, `lint.log`, `formatFix.log`).
- **Clobber-per-run is intentional** — each run overwrites its log so the host always tails the
  *current* build. Do not use `tee -a` (it interleaves runs and grows unbounded).
- **One build at a time per clone.** Two commands writing the same log in the same clone interleave
  and clobber each other. For genuinely concurrent runs in one clone, suffix the name with `$$`
  (e.g. `… | tee .build-logs/compile.$$.log`).
- `.build-logs/` is git-excluded, so it never shows up in commits.

(Because this output is host-visible, prefer it over the baked file's "Reading logs" tips — you don't
need `lnav -n` inside the sandbox here; just tail/lnav the file on the host.)

## If git commands fail

Each directory here is a standalone git clone. If git fails, check the basics:

```bash
git status
git remote -v   # should show origin pointing at github.com/canton-network/splice
```

**Do not attempt to re-clone or manipulate git yourself.** If the clone is broken beyond repair,
tell the user to remove and recreate it from the sbx-templates directory on their HOST:

```bash
export REPO_URL=https://github.com/canton-network/splice
./scripts/remove-clones.sh <branch>
./scripts/setup-clones.sh <branch>
```

## If nix commands fail in a plain (non-template) sandbox

In a template sandbox the daemon starts at boot (see the baked file). In a *plain* sandbox with this
repo mounted, re-run `scripts/bootstrap-nix.sh` from the sbx-templates directory to reinstall nix and
restart the daemon. Daemon logs are at `/tmp/nix-daemon.log`.
