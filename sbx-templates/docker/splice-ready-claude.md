# Splice dev environment (baked into the splice-ready template)

This is baked into the template's user memory (`~/.claude/CLAUDE.md`) so it applies in EVERY
splice-ready sandbox — including `sbx run --clone`, where the repo is cloned fresh into the container
and no project CLAUDE.md is present.

## When asked to build: run these immediately, no exploration needed

```bash
direnv allow                                        # approves .envrc — required on every fresh clone
USER=$(id -un) direnv exec . bash -c \
  'set -o pipefail; mkdir -p .build-logs; stdbuf -oL -eL sbt --batch compile 2>&1 | tee .build-logs/compile.log'
```

Use `direnv exec . bash -c '...'` — not `direnv exec . sbt ...` directly, and never `nix develop`
directly. Do not check `which sbt`. Do not skip `direnv allow`.

`direnv allow` only *trusts* the `.envrc`; it does not keep the env loaded. Each Bash command here
runs in a fresh non-interactive shell with no direnv prompt hook, so the project env (`SBT_OPTS`,
`SPLICE_ROOT`, `PATH`, …) must be re-applied **every** command via `direnv exec .` — a bare `sbt …`
after `direnv allow` runs with none of it.

**Why direnv and not `nix develop path:nix#oss`:** the nix dev shell (`shell.nix` / `path:nix#oss`)
gives you the *toolchain* (sbt, JDK, …) but does **not** set the build-critical env vars that splice
keeps in `.envrc.vars`. direnv loads `.envrc` → `.envrc.vars`; a bare `nix develop` skips it, so you
silently lose:

- `SBT_OPTS` (splice sets `-Xmx16G …`) — without it sbt runs on the ~1 GB default heap and **OOMs**
  on this codebase. If you must run sbt outside direnv, export it yourself.
- `COMETBFT_PROTO=$SPLICE_ROOT/nix/vendored` — needed by the proto build.
- `SPLICE_ROOT` (and the paths derived from it) — the `get-snapshot-version` script run at build init
  fails without it.

`USER` is the one var direnv does *not* set (the sandbox leaves it unset in non-interactive shells),
which is why every command is prefixed with `USER=$(id -un)`.

## Disabling fatal warnings during iteration

`-Xfatal-warnings` is hardcoded in `project/Houserules.scala`. To suppress it during iteration,
create a `local.sbt` at the project root and keep it out of git:

```bash
cat > local.sbt <<'EOF'
ThisBuild / scalacOptions ~= filterNot(_ == "-Xfatal-warnings")
EOF
echo 'local.sbt' >> "$(git rev-parse --git-common-dir)/info/exclude"
```

Remove it before the final pre-PR build so the clean compile confirms no warnings remain.

## sbt project IDs use dashes, not slashes

The directory `apps/app` is the sbt project **`apps-app`**. Use `apps-app/testOnly …`, NOT
`apps/app/testOnly` — a slash fails with "not a valid command". Same for other nested projects
(`apps/common` → `apps-common`, etc.).

## Tests

**Never run bare `sbt test`** — the full suite takes hours. Always scope to a specific test.

**Pre-flight for integration tests** — these cause immediate, confusing failures if missing, *not*
mid-test errors, so check them first:
- nix daemon socket exists: `[ -S /nix/var/nix/daemon-socket/socket ]` (template starts it at boot).
- `direnv allow` has been run this session (otherwise no `SBT_OPTS`/`SPLICE_ROOT` — see above).
- `canton.tokens` exists in the project root. `start-canton.sh` writes it; if it's absent the test
  fails right at config setup. So **start canton first** and let it come up before launching tests.

Integration tests require Canton running first:

```bash
USER=$(id -un) direnv exec . bash -c './start-canton.sh -w -d'   # -w wallclock, -s simtime, -d daemonize
USER=$(id -un) direnv exec . bash -c 'sbt --batch "apps-app/testOnly org.lfdecentralizedtrust.splice.integration.tests.ScanIntegrationTest"'
```

`-w` (wallclock) suits most tests, e.g. ScanIntegrationTest (`simpleTopology1Sv`); simtime tests need
`-s` (see `start-canton.sh --help` / DEVELOPMENT.md). If asked to run tests with no class named, ask which.

Unit / static tests (no Canton): `sbt --batch "apps-app/testOnly com.example.MySpec"`, `sbt --batch damlTest`,
frontend `npm test <file>` (from the frontend dir), helm/pulumi `make cluster/helm/test` /
`make cluster/pulumi/unit-test`.
After adding a test class run `sbt --batch updateTestConfigForParallelRuns` and commit the changed `test-*.log`.
After Daml changes run `sbt updateDarResources` (and `sbt damlDarsLockFileUpdate` if you changed Daml).
After switching branches, if tests fail oddly run `sbt clean-splice` before assuming a real failure.

## Before a PR: format, then lint

```bash
USER=$(id -un) direnv exec . bash -c 'sbt --batch formatFix'   # auto-fixes scalafmt, scalafix, frontend
USER=$(id -un) direnv exec . bash -c 'sbt --batch lint'        # scalafmt, buf, scalafix, shellcheck — must pass clean
```

If `lint` still fails after `formatFix`, fix the rest manually and re-run `lint`.

## Reading logs (canton, build)

Logs are under `log/` (canton.clog, toxi.log, …) and `.build-logs/`. `lnav` is baked into the
template's `agent` profile (on PATH everywhere, no dev shell needed), but it **needs a real TTY** —
Claude Code's shell isn't one, so interactive `lnav` refuses to open. From inside the agent use
headless mode: `lnav -n log/canton.clog` (or `tail`/`grep`). For an interactive view, the human runs
`sbx exec -it <name> bash` then `lnav log/` from a host terminal.

(Use the baked `lnav`, not splice's dev-shell one: `nix/lnav.nix` pins an x86_64-musl build that
won't run on aarch64. More generally — some nix-provided *native* binaries can be the wrong arch;
JVM tools (sbt, java) are wrapper scripts and are fine.)

## Commits

Always sign off (DCO): `git commit -s -m "[static] message"`. Every commit message needs a CI tag:
`[static]` (lint/format only — most commits), `[ci]` (full CI incl. integration), `[skip ci]`,
`[force]`, `[breaking]`. Verify `git config user.name`/`user.email` are set before committing — if
missing, stop and have the user set them (commits can't be signed off otherwise).

## Git & PRs

Push and open PRs against the upstream splice remote — not sbx-templates. In `--clone` mode your
commits live in the container until you push, so push early (`git push -u origin <branch>` first time).

## If nix commands fail

Check the daemon: `[ -S /nix/var/nix/daemon-socket/socket ] && echo running || echo missing`.
In a template sandbox it starts at boot; logs are at `/tmp/nix-daemon.log` (or `/var/log/nix-daemon.log`).
