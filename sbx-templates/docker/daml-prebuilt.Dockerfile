# syntax=docker/dockerfile:1
#
# daml-prebuilt — the template for digital-asset/daml with a FULLY-BAKED bazel build cache. Same nix
# dev-env as daml-ready, but instead of only priming bazel's repository cache it runs a full
# `bazel build //...` at image-build time and bakes the resulting ACTION-OUTPUT cache (--disk_cache)
# into read-only layers. A fresh sandbox then starts with a warm cache and almost all of the fixed
# 20G writable overlay free for iteration.
#
# ─────────────────────────────────────────────────────────────────────────────────────────────────
# PROVENANCE  (what is copied vs modified vs new — you asked for this map explicitly)
# ─────────────────────────────────────────────────────────────────────────────────────────────────
# This is a standalone COPY of docker/daml-ready.Dockerfile (so it slots into build-template.sh's
# existing FROM-base machinery unchanged), with the bazel step upgraded. Concretely:
#
#   INHERITED from nix-direnv-sandbox (docker/nix-direnv.Dockerfile), not reimplemented:
#     • Determinate Nix + flakes, direnv/nix-direnv/lnav in `agent`, nix-daemon auto-start at boot,
#       core nix env persisted to /etc/sandbox-persistent.sh.
#
#   COPIED VERBATIM from daml-ready (the daml-specific nix solution — unchanged here):
#     • §0 /etc/nix/nix.conf → daml's binary cache (nix-cache.da-ext.net) + sandbox=false.
#     • §1 realize daml's pinned dev-env `tools.*` closure into /nix; pre-create dade's nixpkgs
#          snapshot in DADE_VAR_DIR (=/opt/daml-dade-var); realize the sdk/shell.nix closure.
#     • §4 the `daml-bazel-prepare` helper (relocates the writable OUTPUT BASE off the 20G overlay).
#
#   MODIFIED from daml-ready:
#     • §2 was `bazel fetch //...` (repo cache only). Now a full `bazel build //...` that ALSO bakes a
#          --disk_cache, with a size assertion. This is the whole point of this template.
#     • §3 the runtime ~agent/.bazelrc additionally points --disk_cache at the baked CAS and forces
#          --config=linux (so cache keys match — see the invariant below).
#
#   NEW:
#     • §5 bakes runtime user-memory (daml-prebuilt-claude.md) and records the baked ref.
#
# HOW ONE BUILD POPULATES THE DISK CACHE: daml's sdk/.bazelrc wires a COMBINED disk+remote cache
# (build:linux --disk_cache=… + build:linux --remote_cache=…/ubuntu) and build --remote_download_-
# outputs=all. In combined mode the disk cache is a local mirror of the remote: every output bazel
# downloads is written through to the disk cache, and `=all` downloads them all. So a single
# `bazel build //...` with the remote cache reachable fully populates the disk cache, which we bake.
#
# CORRECTNESS INVARIANT: disk-cache keys depend on daml's `--config=linux` flags (nixpkgs host_-
# platform, java17, protocopt, …). The cache is baked UNDER --config=linux, so runtime MUST build
# under the same config or it gets ZERO hits. daml has no platform auto-detect (sdk/.bazelrc), so §3
# forces --config=linux at runtime. Verify on first run: `bazel build <tgt>` should be near-all cache
# hits with no recompilation.
#
# Build:  scripts/build-template.sh daml-prebuilt  ->  workspace/daml-prebuilt.tar  (builds base first)
# Host:   sbx template load workspace/daml-prebuilt.tar
#         sbx run -t daml-prebuilt --name <n> claude <daml-repo-ROOT>   # mount the ROOT, then cd sdk
#         then:  cd sdk && eval "$(dev-env/bin/dade assist)" && daml-bazel-prepare && bazel build //...

ARG BASE=nix-direnv-sandbox:latest
FROM ${BASE}

# Build-time proxy (CONNECT tunnel → no MITM CA needed). Not baked into the runtime image.
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ARG NO_PROXY=
ENV HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY} \
    http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} no_proxy=${NO_PROXY}

# Which daml commit to bake. build-template.sh resolves and passes the CURRENT TIP OF MAIN by default
# (override DAML_REF=<sha> to pin); the literal below is only a fallback for a direct `docker build`.
# Must be a full 40-char SHA — `git fetch origin <ref>` rejects an abbreviated SHA.
ARG DAML_REPO=https://github.com/digital-asset/daml
ARG DAML_REF=e42c3cb1c63e703092bd9bb35c0ee5934c5adeaa

# Targets to build when populating the disk cache. `//...` is the most complete (and heaviest); narrow
# it (e.g. //compiler/...) to trade completeness for a faster, smaller image.
ARG BAZEL_BUILD_TARGETS=//...

# Where the baked caches live (read-only at runtime; see ~/.bazelrc in §3). Cache HITS ride in
# read-only layers and never touch the 20G overlay.
ARG BAZEL_REPO_CACHE=/opt/daml-bazel-cache/repo
ARG BAZEL_DISK_CACHE=/opt/daml-bazel-cache/disk

# Transient output base for the §2 build — deleted before commit, never baked.
ARG BAZEL_BUILD_OUT=/opt/daml-build-out

# Sanity floor: a full //... disk cache is many GB. Smaller ⇒ the remote→disk backfill didn't happen;
# fail the build rather than ship a near-empty cache.
ARG MIN_DISK_CACHE_BYTES=2000000000

# dade's mutable state dir (see daml-ready). Pre-created in §1, exported so `dade assist` reuses it.
ENV DADE_VAR_DIR=/opt/daml-dade-var

USER root
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# === §0. Point nix at daml's binary cache + relax the build sandbox (COPIED from daml-ready) =====
RUN <<'BASH'
set -euo pipefail
mkdir -p /etc/nix
cat >> /etc/nix/nix.conf <<'CONF'
substituters = https://cache.nixos.org https://nix-cache.da-ext.net
trusted-public-keys = cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= hydra.da-int.net-2:91tXuJGf/ExbAz7IWsMsxQ5FsO6lG/EGM5QVt+xhZu0= hydra.da-int.net-1:6Oy2+KYvI7xkAOg0gJisD7Nz/6m8CmyKMbWfSKUe03g= hydra.nixos.org-1:CNHJZBh9K4tP3EKF6FkkgeVYsS3ohTl+oS0Qa8bezVs=
sandbox = false
CONF
BASH

# === §1. Realize daml's nix dev-env closure into /nix (COPIED from daml-ready) ===================
RUN <<'BASH'
set -euxo pipefail

. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
/nix/var/nix/profiles/default/bin/nix-daemon >/tmp/nix-daemon.log 2>&1 &
dpid=$!
for _ in $(seq 1 60); do [ -S /nix/var/nix/daemon-socket/socket ] && break; sleep 1; done
[ -S /nix/var/nix/daemon-socket/socket ] || { cat /tmp/nix-daemon.log; exit 1; }

git init -q /opt/daml
git -C /opt/daml remote add origin "$DAML_REPO"
git -C /opt/daml fetch -q --depth 1 origin "$DAML_REF"
git -C /opt/daml -c advice.detachedHead=false checkout -q FETCH_HEAD

cd /opt/daml/sdk

# (a) Realize the dev-env tools (failure-tolerant: daml builds them lazily, skip the odd uncached one).
mkdir -p /nix/var/nix/gcroots/daml/tools
tools=$(nix-instantiate --eval --strict -E \
  'builtins.concatStringsSep " " (builtins.attrNames (import /opt/daml/sdk/nix/default.nix {}).tools)' \
  | tr -d '"')
for t in $tools; do
  nix-build --keep-going /opt/daml/sdk/nix/default.nix -A "tools.$t" \
    -o "/nix/var/nix/gcroots/daml/tools/$t" \
    || echo "[daml-prebuilt] WARN: tools.$t did not build — skipped (builds lazily at runtime if used)"
done

# (b) Pre-create dade's nixpkgs snapshot + hash so `dade assist` skips its own (broken) realise.
mkdir -p "$DADE_VAR_DIR/gc-roots"
nixpkgs_src=$(nix-instantiate --eval --strict --json \
  -E 'toString (import /opt/daml/sdk/nix/nixpkgs)' | sed 's/^"//;s/"$//')
nix-store --realise --indirect --add-root "$DADE_VAR_DIR/gc-roots/nixpkgs-snapshot" "$nixpkgs_src"
nix-hash /opt/daml/sdk/nix/nixpkgs.nix | tr -d '\n' > "$DADE_VAR_DIR/gc-roots/nixpkgs-snapshot.hash"
chown -R agent "$DADE_VAR_DIR"

# (c) The sdk/shell.nix closure (the `use nix` half of .envrc).
nix print-dev-env --profile /nix/var/nix/profiles/daml-shell -f shell.nix >/dev/null

kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket
BASH

# === §2. Full build → bake the repository cache AND the disk cache (MODIFIED from daml-ready) ====
# daml-ready primed only the repository cache with `bazel fetch`. Here we run a full BUILD so the
# action-output cache is populated too. With daml's combined disk+remote cache (sdk/.bazelrc) and
# --remote_download_outputs=all, every output downloaded from the remote is written through to
# --disk_cache, so this one build fully populates the bakeable CAS.
RUN <<'BASH'
set -euxo pipefail

. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
/nix/var/nix/profiles/default/bin/nix-daemon >/tmp/nix-daemon.log 2>&1 &
dpid=$!
for _ in $(seq 1 60); do [ -S /nix/var/nix/daemon-socket/socket ] && break; sleep 1; done
[ -S /nix/var/nix/daemon-socket/socket ] || { cat /tmp/nix-daemon.log; exit 1; }

mkdir -p "$(dirname "$BAZEL_REPO_CACHE")"
cd /opt/daml/sdk
export USER="${USER:-root}"
eval "$(dev-env/bin/dade assist)"

# --config=linux enables the remote cache (it is :linux-scoped). --remote_download_outputs=all forces
# the write-through that populates --disk_cache. --keep_going + `|| true` so one bad analysis target
# (e.g. daml's scala_repl, rules_scala #18970) doesn't sink the whole image. --output_user_root keeps
# the transient output base out of the baked layers.
bazel --output_user_root="$BAZEL_BUILD_OUT" build "$BAZEL_BUILD_TARGETS" \
  --config=linux \
  --keep_going \
  --repository_cache="$BAZEL_REPO_CACHE" \
  --disk_cache="$BAZEL_DISK_CACHE" \
  --remote_download_outputs=all \
  || echo "[daml-prebuilt] WARN: bazel build reported errors; disk cache holds what built."

bazel --output_user_root="$BAZEL_BUILD_OUT" shutdown || true

# Guard the core assumption: the disk cache must be substantially populated.
sz=$(du -sb "$BAZEL_DISK_CACHE" 2>/dev/null | cut -f1 || echo 0)
echo "[daml-prebuilt] baked disk cache size: $sz bytes (floor: $MIN_DISK_CACHE_BYTES)"
if [ "$sz" -lt "$MIN_DISK_CACHE_BYTES" ]; then
  echo "[daml-prebuilt] FATAL: disk cache far smaller than expected — remote→disk backfill likely" >&2
  echo "                did not happen. Check that --config=linux selects the remote cache, that the" >&2
  echo "                remote cache host was reachable, or build pure-from-source (disable the" >&2
  echo "                remote cache so every action executes locally and writes the disk cache)." >&2
  exit 1
fi

# Runtime user must be able to ADD its own incremental results to the disk cache (writes copy-up to
# the overlay; baked hits stay free in the read-only layer).
chown -R agent "$BAZEL_DISK_CACHE"

# Bake ONLY the two caches. Drop the (huge) transient output base + the source tree (runtime re-clones).
rm -rf "$BAZEL_BUILD_OUT" /opt/daml "${HOME:-/root}/.cache/bazel" /root/.cache/bazel

kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket
BASH

# === §3. Wire bazel to the baked caches at runtime (EXTENDED from daml-ready) ====================
# A home bazelrc is read AFTER the workspace sdk/.bazelrc, so these are the LAST definitions of the
# flags and win over daml's workspace-relative .bazel-cache/{disk,repo}. We force --config=linux on
# user-facing commands so the baked cache (keyed under it) actually hits and the nix toolchains apply.
# Both the unconditional and :linux form of each cache flag are set so we win regardless of --config.
RUN <<'BASH'
set -euo pipefail
cat > /home/agent/.bazelrc <<EOF
# Baked by daml-prebuilt.Dockerfile.

# Force daml's linux config: the baked cache is keyed under it, and daml has no platform auto-detect.
build  --config=linux
test   --config=linux
run    --config=linux
cquery --config=linux

# Baked READ-ONLY action-output cache (the point of this template) — near-all hits, offline.
build       --disk_cache=${BAZEL_DISK_CACHE}
build:linux --disk_cache=${BAZEL_DISK_CACHE}

# Baked READ-ONLY external-dep cache.
build       --repository_cache=${BAZEL_REPO_CACHE}
build:linux --repository_cache=${BAZEL_REPO_CACHE}
fetch       --repository_cache=${BAZEL_REPO_CACHE}
fetch:linux --repository_cache=${BAZEL_REPO_CACHE}
sync        --repository_cache=${BAZEL_REPO_CACHE}
sync:linux  --repository_cache=${BAZEL_REPO_CACHE}
EOF
chown agent /home/agent/.bazelrc
BASH

# === §4. Helper: fit a daml bazel build into the sandbox (COPIED VERBATIM from daml-ready) =======
# bazel's OUTPUT BASE (built artifacts) must be writable, so it can't be a read-only layer. This helper
# relocates it off the 20G overlay (host-mounted disk by default, or the local vdc disk with --vdc) and
# enables build-without-the-bytes — which is SAFE here because intermediates stream from the LOCAL
# baked disk cache, which never evicts.
RUN <<'BASH'
set -euo pipefail
cat > /usr/local/bin/daml-bazel-prepare <<'EOF'
#!/usr/bin/env bash
# daml-bazel-prepare [--vdc] — relocate bazel's big WRITABLE state off the fixed 20G root overlay so a
# daml build fits & runs fast. Run from the daml bazel workspace (sdk/); safe to re-run (it rewrites
# its own managed block in %workspace%/.bazelrc.local, which daml try-imports and which is git-ignored).
set -euo pipefail

mode=host
case "${1:-}" in
  --vdc) mode=vdc ;;
  "")    ;;
  *)     echo "usage: daml-bazel-prepare [--vdc]" >&2; exit 2 ;;
esac

ws="$PWD"
while [ "$ws" != / ] && [ ! -e "$ws/WORKSPACE" ] && [ ! -e "$ws/WORKSPACE.bazel" ]; do
  ws="$(dirname "$ws")"
done
[ "$ws" = / ] && { echo "daml-bazel-prepare: no WORKSPACE found above $PWD — run me from daml's sdk/ dir." >&2; exit 1; }

rc="$ws/.bazelrc.local"
touch "$rc"

extra=""
if [ "$mode" = vdc ]; then
  out=/var/lib/docker/daml-bazel-out
  [ -d /var/lib/docker ] || { echo "daml-bazel-prepare --vdc: /var/lib/docker missing — wrong sandbox?" >&2; exit 1; }
  if [ "$(df -P / | awk 'NR==2{print $1}')" = "$(df -P /var/lib/docker | awk 'NR==2{print $1}')" ]; then
    echo "daml-bazel-prepare --vdc: /var/lib/docker is on the SAME disk as / (no separate vdc here) — won't add space." >&2
    exit 1
  fi
  if ! { sudo -n chmod o+x /var/lib/docker \
      && sudo -n mkdir -p "$out" \
      && sudo -n chown "$(id -un)" "$out"; }; then
    echo "daml-bazel-prepare --vdc: could not provision $out (needs passwordless sudo)." >&2
    exit 1
  fi
else
  out="$ws/.bazel-cache/output"
  mkdir -p "$out"
  sbox=/tmp/daml-bazel-sandbox; mkdir -p "$sbox"
  extra="build --sandbox_base=$sbox"
fi

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
echo "  intermediates stream from the BAKED local disk cache (offline, no eviction)."
echo "  still too slow? add 'build --spawn_strategy=local' (less hermetic) to $rc OUTSIDE the managed block."
EOF
chmod +x /usr/local/bin/daml-bazel-prepare
BASH

# === §5. Record the baked ref + bake runtime user-memory (NEW) ==================================
RUN echo "$DAML_REF" > /etc/daml-prebuilt.ref

# Claude Code loads ~/.claude/CLAUDE.md regardless of cwd, so EVERY daml-prebuilt sandbox gets this —
# including `sbx run --clone`, where the cloned daml repo carries no project CLAUDE.md. build-template.sh
# stages docker/daml-prebuilt-claude.md into the build context for this COPY.
COPY --chown=agent:agent daml-prebuilt-claude.md /home/agent/.claude/CLAUDE.md

# Don't bake the build-time proxy into the runtime image.
ENV HTTP_PROXY= HTTPS_PROXY= NO_PROXY= http_proxy= https_proxy= no_proxy=

# Restore the runtime user/dir (entrypoint + cmd — incl. nix-daemon autostart — inherited from base).
USER agent
WORKDIR /home/agent/workspace
