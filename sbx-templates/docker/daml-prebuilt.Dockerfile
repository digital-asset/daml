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
# records the baked caches in ~/.bazelrc. --config=linux itself is injected by dev-env/bin/bazel
# (the nix wrapper) via `--bazelrc /nix/store/…/daml-bazelrc`; do NOT add it again in ~/.bazelrc
# or the §2 build command — that doubles every :linux config flag and breaks protoc (duplicate
# --include_source_info).  Verify on first run: `bazel build <tgt>` should be near-all cache hits.
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

# dev-env/bin/bazel (the nix wrapper) already injects `build --config linux` via its own --bazelrc;
# do NOT pass --config=linux here too — it doubles every :linux flag (including protocopt) and breaks
# protoc actions that need to run locally. --remote_download_outputs=all forces the write-through
# that populates --disk_cache. --keep_going + `|| true` so one bad analysis target (e.g. daml's
# scala_repl, rules_scala #18970) doesn't sink the whole image. --output_user_root keeps the
# transient output base out of the baked layers.
bazel --output_user_root="$BAZEL_BUILD_OUT" build "$BAZEL_BUILD_TARGETS" \
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

# Runtime user must own BOTH baked caches. The disk cache: so it can ADD its own incremental results
# (writes copy-up to the overlay; baked hits stay free in the read-only layer). The repo cache: Bazel 7
# creates temp files inside the SHA256 dirs even on a pure cache HIT, so a root-owned read-only repo
# cache fails the fetch — it must be writable by the runtime `agent` user.
chown -R agent "$BAZEL_DISK_CACHE" "$BAZEL_REPO_CACHE"

# Bake ONLY the two caches. Drop the (huge) transient output base + the source tree (runtime re-clones).
rm -rf "$BAZEL_BUILD_OUT" /opt/daml "${HOME:-/root}/.cache/bazel" /root/.cache/bazel

kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket
BASH

# === §3. Wire bazel to the baked caches at runtime (EXTENDED from daml-ready) ====================
# A home bazelrc is read AFTER the workspace sdk/.bazelrc, so these are the LAST definitions of the
# flags and win over daml's workspace-relative .bazel-cache/{disk,repo}.
# NOTE: do NOT add `build --config=linux` here. dev-env/bin/bazel (the nix wrapper that dade assist
# puts on PATH) already injects `build --config linux` via `--bazelrc /nix/store/…/daml-bazelrc`.
# Adding it again doubles every build:linux flag (disk_cache, repository_cache, protocopt, …) and
# breaks protoc with "may only be passed once". The baked disk cache uses the key set from a SINGLE
# --config=linux expansion; adding a second one causes complete cache-key mismatches (zero hits).
# Both the unconditional and :linux form of each cache flag are set so we win regardless of --config.
RUN <<'BASH'
set -euo pipefail
cat > /home/agent/.bazelrc <<EOF
# Baked by daml-prebuilt.Dockerfile.
# --config=linux is NOT listed here because dev-env/bin/bazel (the nix wrapper) already injects it.
# Adding it here a second time would double every :linux flag and break protoc + invalidate cache keys.

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

# === §4. Helper: fit a daml bazel build into the sandbox (adapted from daml-ready) ===============
# bazel's OUTPUT BASE (built artifacts) must be writable, so it can't be a read-only layer. This helper
# relocates it off the 20G overlay (host-mounted disk by default, or the local vdc disk with --vdc).
# It sets --remote_download_outputs=all (NOT build-without-the-bytes): the baked disk cache doesn't
# hold every intermediate and daml's remote CAS evicts, so BwoB (=toplevel) hits CacheNotFoundException
# — =all materializes every output from the offline baked cache instead. The relocated, roomy output
# base is what makes full materialization fit.
#
# DAML-PREBUILT MODE: When /etc/daml-prebuilt.ref exists, this script uses --output_base (exact path)
# instead of --output_user_root (parent where Bazel computes a hash subdirectory). This is critical:
# the baked disk cache was built with output_base at /opt/daml-build-out/<hash-of-/opt/daml/sdk>.
# Runtime workspaces mount at different paths (e.g. /home/<user>/daml/sdk), which would compute a
# DIFFERENT hash and miss the cache entirely. By setting --output_base to the EXACT baked path, cache
# keys match regardless of where the workspace is mounted.
RUN <<'BASH'
set -euo pipefail
cat > /usr/local/bin/daml-bazel-prepare <<'EOF'
#!/usr/bin/env bash
# daml-bazel-prepare [--vdc] — relocate bazel's big WRITABLE state off the fixed 20G root overlay so a
# daml build fits & runs fast. Run from the daml bazel workspace (sdk/); safe to re-run (it rewrites
# its own managed block in %workspace%/.bazelrc.local, which daml try-imports and which is git-ignored).
#
# In daml-prebuilt sandboxes, this script uses --output_base (exact path) to hit the baked disk cache
# regardless of where the workspace is mounted.
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

# Detect daml-prebuilt mode: use the EXACT baked output_base for cache hits.
# The baked cache was built at /opt/daml/sdk → output_base /opt/daml-build-out/<hash>.
# We find that hash directory dynamically rather than hardcoding it.
use_output_base=false
if [ -f /etc/daml-prebuilt.ref ]; then
  # Find the baked output base directory (there's exactly one 32-char hex-named subdir)
  baked_out=$(find /opt/daml-build-out -maxdepth 1 -mindepth 1 -type d -name '[0-9a-f]*' 2>/dev/null | head -1)
  if [ -n "$baked_out" ] && [ -d "$baked_out" ]; then
    use_output_base=true
    out="$baked_out"
    # Ensure writable (it's on the overlay, but may be root-owned from image build)
    sudo -n chown "$(id -un)" "$out" 2>/dev/null || true
    echo "daml-bazel-prepare: detected daml-prebuilt image"
    echo "  using baked output_base: $out"
  else
    echo "daml-bazel-prepare: WARNING: /etc/daml-prebuilt.ref exists but no baked output_base found" >&2
    echo "  falling back to standard mode (cache may not hit)" >&2
  fi
fi

extra=""
if [ "$use_output_base" = false ]; then
  # Standard mode: use --output_user_root (Bazel computes hash subdirectory)
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
fi

# Claude Code exports TMPDIR=/tmp/claude-<uid>; Bazel 7's hermetic-tmp sandbox overmounts /tmp with a
# fresh tmpfs, so that subdir doesn't exist inside the sandbox and actions that honor TMPDIR fail.
# Bind-mount the real TMPDIR into the sandbox so it exists there. Only when TMPDIR is set and isn't
# plain /tmp (i.e. only when something like Claude Code redirected it) — so non-Claude builds keep a
# fully hermetic /tmp and nothing is added.
tmpmount=""
if [ -n "${TMPDIR:-}" ] && [ "$TMPDIR" != /tmp ]; then
  tmpmount="build --sandbox_add_mount_pair=$TMPDIR"
fi

tmp="$(mktemp)"
sed -E \
  -e '/^# >>> daml-bazel-prepare/,/^# <<< daml-bazel-prepare/d' \
  -e '/^startup --output_base=/d' \
  -e '/^startup --output_user_root=/d' \
  -e '/^build --remote_download_outputs=/d' \
  -e '/^build --sandbox_base=/d' \
  -e '/^build --sandbox_add_mount_pair=/d' \
  -e '/^build --reuse_sandbox_directories$/d' \
  "$rc" > "$tmp"
{
  cat "$tmp"
  echo "# >>> daml-bazel-prepare ($mode; managed — re-run to change, don't edit inside) >>>"
  if [ "$use_output_base" = true ]; then
    # daml-prebuilt: use EXACT output_base to match baked cache keys
    echo "startup --output_base=$out"
  else
    # standard: use output_user_root (Bazel computes hash subdirectory)
    echo "startup --output_user_root=$out"
  fi
  # Materialize ALL build outputs locally, NOT build-without-the-bytes (=toplevel). The baked disk
  # cache does not hold every intermediate, and daml's remote CAS evicts, so BwoB hits
  # CacheNotFoundException; =all forces every output down from the (offline) baked cache instead.
  echo "build --remote_download_outputs=all"
  [ -n "$extra" ] && echo "$extra"
  [ -n "$tmpmount" ] && echo "$tmpmount"
  echo "build --reuse_sandbox_directories"
  echo "# <<< daml-bazel-prepare <<<"
} > "$rc"
rm -f "$tmp"

echo "daml-bazel-prepare ($mode): configured $rc"
echo "  output base -> $out  ($(df -Ph "$out" | awk 'NR==2 {print $4" free on "$6}'))"
if [ "$use_output_base" = true ]; then
  echo "  daml-prebuilt mode: using exact --output_base for baked cache compatibility."
elif [ "$mode" = vdc ]; then
  echo "  vdc is the nested-Docker disk: fast + local, but sandbox-lifetime only and shared with docker."
else
  echo "  host mount is virtiofs (slower); try --vdc for the fast local disk if you're not using nested Docker."
fi
echo "  all outputs are materialized from the BAKED local disk cache (offline; =all, not BwoB)."
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
