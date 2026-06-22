# syntax=docker/dockerfile:1
#
# daml-ready — the template for digital-asset/daml. The generic `nix-direnv-sandbox` base with
# daml's PINNED nix dev-env closure baked into /nix as read-only layers, plus a primed bazel
# repository cache. All daml-specific content lives ONLY in this layer; the public base has none.
#
# daml is bigger than splice and uses nix AND bazel:
#   - nix provides the toolchain (GHC, JDK17, Scala, Node, Python, the bazel_7 wrapper, …). That
#     closure is realized below and rides in read-only image layers, so it never touches the 20G
#     writable overlay — the sandbox's small root disk is irrelevant.
#   - bazel pulls gigabytes of EXTERNAL deps (maven, stackage/haskell, npm/yarn, go, http_archives).
#     We prime bazel's --repository_cache into a read-only baked dir so a fresh sandbox doesn't
#     re-download them. Built ACTION OUTPUTS are NOT baked (bazel must write its output base at
#     runtime); they go to the overlay, mitigated by daml's remote cache (bazel-cache.da-ext.net)
#     and the workspace --disk_cache. A full `bazel build //...` output base may not fit 20G — scope
#     builds to specific targets.
#
# daml's dev env lives under the repo's `sdk/` subdirectory. `sdk/.envrc` runs `use nix`
# (realizes sdk/shell.nix) AND `eval "$(dev-env/bin/dade assist)"` (the dade tool system, which
# lazily realizes `nix-build sdk/nix/default.nix -A tools.<name>`). We bake the full `tools` closure
# so dade's per-tool nix-build is an instant cache hit at runtime.
#
# Build:  scripts/build-template.sh daml   ->  workspace/daml-ready.tar   (builds the base first)
# Host:   sbx template load workspace/daml-ready.tar
#         sbx run -t daml-ready --name <n> claude <daml-repo-ROOT>   # mount the ROOT (so .git is in),
#         then `cd sdk` inside — NOT `claude <repo>/sdk` (that leaves the repo-root .git unmounted).
#         build with (no direnv — the dev-env wrappers self-bootstrap):
#           cd sdk && eval "$(dev-env/bin/dade assist)" && daml-bazel-prepare && bazel build //...

ARG BASE=nix-direnv-sandbox:latest
FROM ${BASE}

# Build-time proxy (CONNECT tunnel → no MITM CA needed). Not baked into the runtime image.
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ARG NO_PROXY=
ENV HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY} \
    http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} no_proxy=${NO_PROXY}

# Which daml commit to bake. nix/src.json + nix/nixpkgs/ under this ref pin everything.
# Full 40-char SHA — `git fetch origin <ref>` rejects an abbreviated SHA ("couldn't find remote ref").
ARG DAML_REPO=https://github.com/digital-asset/daml
ARG DAML_REF=e42c3cb1c63e703092bd9bb35c0ee5934c5adeaa

# Bazel targets to fetch when priming the repository cache. `//...` is the most complete (and the
# heaviest — many GB, long); narrow it (e.g. `//daml-assistant/...`) to trade completeness for speed.
ARG BAZEL_FETCH_TARGETS=//...

# Where the primed bazel repository cache is baked (read-only at runtime; see ~/.bazelrc below).
ARG BAZEL_REPO_CACHE=/opt/daml-bazel-cache/repo

# dade's mutable state dir. We pre-create its nixpkgs snapshot here at build time and export this at
# runtime so `dade assist` reuses it instead of running its OWN snapshot realise — that realise is
# broken under Determinate Nix (`nix-instantiate --eval nixpkgs.nix -A path` prints the fetchTarball
# URL, not a store path). Baked into the image; the runtime overlay makes it writable so dade can add
# its tool gc-roots here too. Lives OUTSIDE /opt/daml (which is removed after the bazel fetch).
ENV DADE_VAR_DIR=/opt/daml-dade-var

USER root
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# --- 0. Point nix at daml's binary cache + relax the build sandbox ------------------------------
# daml's dev-env packages are served from daml's OWN binary cache (sdk/dev-env/etc/nix.conf); many
# are absent from cache.nixos.org for this pinned (EOL python3.9) nixpkgs. Configure that substituter
# so the closure is fetched prebuilt. Also disable the build sandbox: a few dev-env tools that DO
# build from source run nixpkgs checkPhases with localhost-networking tests (e.g. sphinx's
# test_invalid_ssl) that FAIL under the restricted nix sandbox but PASS unsandboxed. Disabling the
# sandbox keeps the canonical store-path hashes (so runtime reuses them), it only relaxes the build
# environment. This file persists across the RUN layers below. (Written once, here.)
RUN <<'BASH'
set -euo pipefail
mkdir -p /etc/nix
cat >> /etc/nix/nix.conf <<'CONF'
substituters = https://cache.nixos.org https://nix-cache.da-ext.net
trusted-public-keys = cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= hydra.da-int.net-2:91tXuJGf/ExbAz7IWsMsxQ5FsO6lG/EGM5QVt+xhZu0= hydra.da-int.net-1:6Oy2+KYvI7xkAOg0gJisD7Nz/6m8CmyKMbWfSKUe03g= hydra.nixos.org-1:CNHJZBh9K4tP3EKF6FkkgeVYsS3ohTl+oS0Qa8bezVs=
sandbox = false
CONF
BASH

# --- 1. Realize daml's nix dev-env closure into /nix (read-only layers) -------------------------
RUN <<'BASH'
set -euxo pipefail

# Nix + flakes are already installed by the base; bring the daemon up (now reading the config above).
. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
/nix/var/nix/profiles/default/bin/nix-daemon >/tmp/nix-daemon.log 2>&1 &
dpid=$!
for _ in $(seq 1 60); do [ -S /nix/var/nix/daemon-socket/socket ] && break; sleep 1; done
[ -S /nix/var/nix/daemon-socket/socket ] || { cat /tmp/nix-daemon.log; exit 1; }

# Clone daml at the pinned ref.
git init -q /opt/daml
git -C /opt/daml remote add origin "$DAML_REPO"
git -C /opt/daml fetch -q --depth 1 origin "$DAML_REF"
git -C /opt/daml -c advice.detachedHead=false checkout -q FETCH_HEAD

cd /opt/daml/sdk

# (a) Realize the dev-env tools so `dade assist`'s per-tool nix-build is an instant hit at runtime.
#     Build each tool INDIVIDUALLY and tolerate failures: daml builds these LAZILY, so a few
#     rarely-used ones may neither be cached nor build cleanly — skip those rather than fail the whole
#     image (they build lazily at runtime if ever invoked). The essential toolchain (bazel, jdk, ghc,
#     scala, node, protoc, …) builds/substitutes fine and IS rooted here.
mkdir -p /nix/var/nix/gcroots/daml/tools
tools=$(nix-instantiate --eval --strict -E \
  'builtins.concatStringsSep " " (builtins.attrNames (import /opt/daml/sdk/nix/default.nix {}).tools)' \
  | tr -d '"')
for t in $tools; do
  nix-build --keep-going /opt/daml/sdk/nix/default.nix -A "tools.$t" \
    -o "/nix/var/nix/gcroots/daml/tools/$t" \
    || echo "[daml-ready] WARN: tools.$t did not build — skipped (builds lazily at runtime if used)"
done

# (b) Pre-create dade's nixpkgs snapshot + its hash file in $DADE_VAR_DIR, so `dade assist` at runtime
#     (and in step 2 below) SKIPS its own snapshot realise. dade only realises when the snapshot is
#     missing or its hash differs (sdk/dev-env/lib/dade-dump-profile); we satisfy both. We root the
#     nixpkgs SOURCE directly with `toString` (an unambiguous store path) instead of `nixpkgs.nix -A
#     path`, which prints the fetchTarball URL under Determinate Nix. The hash must match dade's
#     `nix-hash <repo>/nix/nixpkgs.nix` exactly — same (pinned) file content + same nix → same hash.
mkdir -p "$DADE_VAR_DIR/gc-roots"
# `toString` coerces the fetchTarball result to its realized store path; --json prints it cleanly as
# "/nix/store/…". nix-instantiate is impure by default, so fetchTarball is allowed.
nixpkgs_src=$(nix-instantiate --eval --strict --json \
  -E 'toString (import /opt/daml/sdk/nix/nixpkgs)' | sed 's/^"//;s/"$//')
nix-store --realise --indirect --add-root "$DADE_VAR_DIR/gc-roots/nixpkgs-snapshot" "$nixpkgs_src"
nix-hash /opt/daml/sdk/nix/nixpkgs.nix | tr -d '\n' > "$DADE_VAR_DIR/gc-roots/nixpkgs-snapshot.hash"
# Writable by the runtime user so dade can add tool gc-roots here (overlay copy-up keeps root-owned
# baked files unwritable otherwise).
chown -R agent "$DADE_VAR_DIR"

# (c) The sdk/shell.nix closure (the `use nix` half of .envrc) — realized at runtime by `use nix`.
nix print-dev-env --profile /nix/var/nix/profiles/daml-shell -f shell.nix >/dev/null

kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket
BASH

# --- 2. Prime bazel's repository cache into a read-only baked dir --------------------------------
# Run a fetch with the dev-env tools on PATH (via `dade assist`), pointing --repository_cache at the
# baked location. This captures the NON-nix downloads; nix-sourced deps are already in /nix above.
# WORKSPACE-mode (daml sets `common --noenable_bzlmod`). `--config=linux` selects daml's linux
# toolchains/host_platform for fetch. If `//...` is too heavy, override BAZEL_FETCH_TARGETS.
RUN <<'BASH'
set -euxo pipefail

. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
/nix/var/nix/profiles/default/bin/nix-daemon >/tmp/nix-daemon.log 2>&1 &
dpid=$!
for _ in $(seq 1 60); do [ -S /nix/var/nix/daemon-socket/socket ] && break; sleep 1; done
[ -S /nix/var/nix/daemon-socket/socket ] || { cat /tmp/nix-daemon.log; exit 1; }

mkdir -p "$(dirname "$BAZEL_REPO_CACHE")"

cd /opt/daml/sdk
# dade assist sets up PATH to dev-env/bin wrappers (bazel, jdk, …); the baked closure makes each
# wrapper's nix-build an instant hit. Run as root (bazel tolerates it); $USER may be unset in build.
export USER="${USER:-root}"
eval "$(dev-env/bin/dade assist)"

# Priming the repo cache is best-effort: `fetch //...` does full LOADING+ANALYSIS to discover deps,
# so a single target that fails ANALYSIS (e.g. daml's scala_repl targets hit rules_scala/Bazel
# issue #18970) would otherwise abort it — even though most external repos are already downloaded.
# --keep_going fetches as much as possible; `|| true` keeps the build going and bakes what was
# fetched. (Runtime builds scope to specific targets and don't hit these analysis-only edge targets.)
bazel fetch "$BAZEL_FETCH_TARGETS" \
  --config=linux \
  --keep_going \
  --repository_cache="$BAZEL_REPO_CACHE" \
  || echo "[daml-ready] WARN: bazel fetch reported errors (some targets failed analysis); repository cache primed with what was fetched."

# Stop bazel's server so no daemon/lock rides into the committed layer.
bazel shutdown || true

# Keep ONLY the primed repository cache. Drop everything else bazel wrote so it doesn't bloat the
# layer: the output base (~/.cache/bazel — built action outputs, NOT baked) and the source tree
# (the runtime workspace is a fresh clone). gc-roots/profiles point into /nix store paths, not these,
# so removing them is safe.
rm -rf "${HOME:-/root}/.cache/bazel" /root/.cache/bazel
rm -rf /opt/daml

kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket
BASH

# --- 3. Wire bazel to the baked repository cache at runtime --------------------------------------
# A home bazelrc is read AFTER the workspace .bazelrc, so its --repository_cache wins over daml's
# default workspace-relative `.bazel-cache/repo`. The baked cache is a READ-ONLY SEED: pre-baked deps
# are served as hits; a genuinely new dep downloads fresh (bazel warns it can't write the cache, then
# proceeds). Built action outputs still come from daml's remote cache + the workspace --disk_cache.
RUN <<'BASH'
set -euo pipefail
cat > /home/agent/.bazelrc <<EOF
# Baked by daml-ready.Dockerfile — point bazel at the read-only primed repository cache.
build --repository_cache=${BAZEL_REPO_CACHE}
fetch --repository_cache=${BAZEL_REPO_CACHE}
sync  --repository_cache=${BAZEL_REPO_CACHE}
EOF
chown agent /home/agent/.bazelrc
BASH

# --- 4. Helper: fit a daml bazel build into the sandbox -----------------------------------------
# bazel's OUTPUT BASE (built artifacts) MUST be writable, so it can't be a read-only layer — by
# default it lands in ~/.cache/bazel on the fixed 20G root overlay and a daml build overflows it.
# This helper relocates the output base off the overlay and enables build-without-the-bytes. Two
# targets: the HOST-MOUNTED workspace disk (default — roomy but slow virtiofs) or the sandbox's LOCAL
# vdc disk at /var/lib/docker (`--vdc` — fast + ~roomy, but the nested-Docker disk). The CLAUDE.md
# build preamble runs it from the daml `sdk/` dir before building.
RUN <<'BASH'
set -euo pipefail
cat > /usr/local/bin/daml-bazel-prepare <<'EOF'
#!/usr/bin/env bash
# daml-bazel-prepare [--vdc] — relocate bazel's big WRITABLE state off the fixed 20G root overlay so a
# daml build fits & runs fast. Run from the daml bazel workspace (sdk/); safe to re-run (it rewrites
# its own managed block in %workspace%/.bazelrc.local, which daml try-imports and which is git-ignored).
#
#   (default)  output base -> <ws>/.bazel-cache/output  on the HOST-MOUNTED disk. Roomy, but that mount
#              is virtiofs (slow), so we also move the per-action sandbox symlink forest onto the fast
#              local overlay (/tmp). Persists across sandbox restarts.
#   --vdc      output base -> /var/lib/docker/daml-bazel-out  on the sandbox's LOCAL vdc disk. FAST
#              (local, no virtiofs) and ~roomy (it's the 50G nested-Docker disk). Caveats: shared with
#              any nested-Docker images, and it only persists for the sandbox's lifetime (a recreate
#              wipes it). Best when you're not using nested Docker and want speed.
set -euo pipefail

mode=host
case "${1:-}" in
  --vdc) mode=vdc ;;
  "")    ;;
  *)     echo "usage: daml-bazel-prepare [--vdc]" >&2; exit 2 ;;
esac

# Locate the bazel workspace (dir containing WORKSPACE), walking up from CWD.
ws="$PWD"
while [ "$ws" != / ] && [ ! -e "$ws/WORKSPACE" ] && [ ! -e "$ws/WORKSPACE.bazel" ]; do
  ws="$(dirname "$ws")"
done
[ "$ws" = / ] && { echo "daml-bazel-prepare: no WORKSPACE found above $PWD — run me from daml's sdk/ dir." >&2; exit 1; }

rc="$ws/.bazelrc.local"
touch "$rc"

extra=""   # mode-specific extra `build` lines
if [ "$mode" = vdc ]; then
  out=/var/lib/docker/daml-bazel-out
  [ -d /var/lib/docker ] || { echo "daml-bazel-prepare --vdc: /var/lib/docker missing — wrong sandbox?" >&2; exit 1; }
  if [ "$(df -P / | awk 'NR==2{print $1}')" = "$(df -P /var/lib/docker | awk 'NR==2{print $1}')" ]; then
    echo "daml-bazel-prepare --vdc: /var/lib/docker is on the SAME disk as / (no separate vdc here) — won't add space." >&2
    exit 1
  fi
  # /var/lib/docker is root-owned drwx--x--- (docker locks it down). Add o+x so a non-root user can
  # traverse INTO its own subdir (not list the dir), and create an agent-owned output dir on the vdc fs.
  # Docker's daemon runs as root, so the o+x is harmless to it.
  if ! { sudo -n chmod o+x /var/lib/docker \
      && sudo -n mkdir -p "$out" \
      && sudo -n chown "$(id -un)" "$out"; }; then
    echo "daml-bazel-prepare --vdc: could not provision $out (needs passwordless sudo)." >&2
    exit 1
  fi
  # output base is already on the fast local vdc disk, so leave the sandbox forest under it (default).
else
  out="$ws/.bazel-cache/output"      # .bazel-cache is in daml's .bazelignore + .gitignore
  mkdir -p "$out"
  sbox=/tmp/daml-bazel-sandbox; mkdir -p "$sbox"
  # host mount is virtiofs (slow): stage the per-action symlink forest on the fast local overlay.
  extra="build --sandbox_base=$sbox"
fi

# Rewrite the managed block idempotently: strip our previous lines (block markers AND any bare lines
# from older versions / other mode), then append a fresh block. User's own lines are preserved.
tmp="$(mktemp)"
sed -E \
  -e '/^# >>> daml-bazel-prepare/,/^# <<< daml-bazel-prepare/d' \
  -e '/^startup --output_user_root=/d' \
  -e '/^build --remote_download_outputs=/d' \
  -e '/^build --sandbox_base=/d' \
  -e '/^build --reuse_sandbox_directories$/d' \
  "$rc" > "$tmp"
{
  cat "$tmp"
  echo "# >>> daml-bazel-prepare ($mode; managed — re-run to change, don't edit inside) >>>"
  echo "startup --output_user_root=$out"
  echo "build --remote_download_outputs=toplevel"
  [ -n "$extra" ] && echo "$extra"
  echo "build --reuse_sandbox_directories"
  echo "# <<< daml-bazel-prepare <<<"
} > "$rc"
rm -f "$tmp"

echo "daml-bazel-prepare ($mode): configured $rc"
echo "  output base -> $out  ($(df -Ph "$out" | awk 'NR==2 {print $4" free on "$6}'))"
if [ "$mode" = vdc ]; then
  echo "  vdc is the nested-Docker disk: fast + local, but sandbox-lifetime only and shared with docker."
else
  echo "  host mount is virtiofs (slower); try --vdc for the fast local disk if you're not using nested Docker."
fi
echo "  remote cache needs:  sbx policy allow network bazel-cache.da-ext.net"
echo "  still too slow? add 'build --spawn_strategy=local' (less hermetic) to $rc OUTSIDE the managed block."
EOF
chmod +x /usr/local/bin/daml-bazel-prepare
BASH

# Don't bake the build-time proxy into the runtime image.
ENV HTTP_PROXY= HTTPS_PROXY= NO_PROXY= http_proxy= https_proxy= no_proxy=

# Restore the runtime user/dir (entrypoint + cmd — incl. nix-daemon autostart — inherited from base).
USER agent
WORKDIR /home/agent/workspace
