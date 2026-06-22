#!/usr/bin/env bash
# build-template.sh — build a sbx template image and export it to a tar you load on the HOST.
#
# Usage:
#   ./scripts/build-template.sh base      # nix-direnv-sandbox  (generic, shareable, no private bits)
#   ./scripts/build-template.sh splice    # splice-ready (FROM the base + splice's pinned OSS closure)
#   ./scripts/build-template.sh daml      # daml-ready   (FROM the base + daml's pinned nix dev-env +
#                                          #               primed bazel repository cache)
#   ./scripts/build-template.sh           # default: splice (builds the base first if missing)
#
# Templates bake their toolchain into READ-ONLY image layers, so /nix never consumes the 20G
# writable overlay — works regardless of the sandbox root-disk size.
#
# The big (splice/daml) images are exported with `--output type=oci` straight to a tar on the host
# mount, which AVOIDS unpacking a second large copy into the nested-docker image store (two copies of
# a big store do not fit on vdc). The small base is loaded into the store (so the others can build
# FROM it) and `docker save`d.
#
# After it finishes, on the HOST:
#   sbx template load workspace/<image>.tar
#   sbx run -t <image> --name <new-name> claude <fresh-workspace>   # daml: mount the repo ROOT, then cd sdk
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-splice}"

BASE_TAG="${BASE_TAG:-nix-direnv-sandbox:latest}"

SPLICE_TAG="${SPLICE_TAG:-splice-ready:latest}"
SPLICE_REPO="${SPLICE_REPO:-https://github.com/canton-network/splice}"
SPLICE_REF="${SPLICE_REF:-d1d5dc53951e893deabdddeb38533fe2841f3cfb}"   # full SHA (abbrev is rejected by git fetch); incl. lnav aarch64-linux fix (#6070)

DAML_TAG="${DAML_TAG:-daml-ready:latest}"
DAML_REPO="${DAML_REPO:-https://github.com/digital-asset/daml}"
DAML_REF="${DAML_REF:-e42c3cb1c63e703092bd9bb35c0ee5934c5adeaa}"        # full SHA (abbrev is rejected by git fetch)
# Bazel targets fetched when priming daml's repository cache. `//...` is the most complete and the
# heaviest; narrow it (e.g. //daml-assistant/...) to trade completeness for a faster, smaller build.
BAZEL_FETCH_TARGETS="${BAZEL_FETCH_TARGETS:-//...}"

# The docker driver doesn't support --output type=oci; a docker-container driver builder is required.
# That builder runs in its own container and can't see the host daemon's image store, so we expose
# the locally-built base image via a short-lived local registry (localhost, always treated as insecure).
BUILDER_NAME="nix-try-oci-builder"
REG_CNAME="nix-try-build-reg"
REG_PORT=15000   # high port to avoid conflicts with any local registry:5000

log() { printf '\033[1;34m[build-template]\033[0m %s\n' "$*"; }
command -v docker >/dev/null || { echo "docker (nested daemon) not found" >&2; exit 1; }

PROXY_ARGS=(
  --build-arg HTTP_PROXY="${HTTP_PROXY:-${http_proxy:-}}"
  --build-arg HTTPS_PROXY="${HTTPS_PROXY:-${https_proxy:-}}"
  --build-arg NO_PROXY="${NO_PROXY:-${no_proxy:-}}"
)

# The Dockerfiles clone their repo INSIDE the build, so the context only needs the few small assets
# they COPY in (e.g. the baked CLAUDE.md) — never the huge host-mounted workspace/. new_ctx() makes
# an empty dir; stage_ctx_assets() drops docker/*.md into it for the COPY steps.
new_ctx() { mktemp -d; }
stage_ctx_assets() { cp "$HERE"/docker/*.md "$1"/ 2>/dev/null || true; }

REGDATA=""

_cleanup_oci_resources() {
  docker buildx rm "$BUILDER_NAME" >/dev/null 2>&1 || true
  docker rm -f "$REG_CNAME" >/dev/null 2>&1 || true
  [ -n "$REGDATA" ] && rm -rf "$REGDATA"
}
trap '_cleanup_oci_resources' EXIT INT TERM

# Build the small base into the local image store (so the others can build FROM it) and save its tar.
build_base() {
  local ctx; ctx="$(new_ctx)"
  log "building $BASE_TAG (generic base) into the local store…"
  docker buildx build -f "$HERE/docker/nix-direnv.Dockerfile" "${PROXY_ARGS[@]}" \
    --load -t "$BASE_TAG" "$ctx"
  rm -rf "$ctx"
}

# Make sure the base image is in the local store: load from its tar if present, else build it.
ensure_base_loaded() {
  if docker image inspect "$BASE_TAG" >/dev/null 2>&1; then return; fi
  if [ -f "$HERE/workspace/nix-direnv-sandbox.tar" ]; then
    log "loading base $BASE_TAG from workspace/nix-direnv-sandbox.tar (avoids rebuilding the base)…"
    docker load -i "$HERE/workspace/nix-direnv-sandbox.tar"
  else
    log "base $BASE_TAG missing and no base tar — building it first."
    build_base
  fi
}

# Reclaim the nested-docker disk after exporting (the tar is the deliverable). KEEP_IMAGE=1 keeps it.
reclaim() {
  [ "${KEEP_IMAGE:-0}" = 1 ] && { log "KEEP_IMAGE=1 — leaving images in the nested daemon."; return; }
  log "reclaiming nested-docker space (rmi $* + prune cache)…"
  docker rmi -f "$@" >/dev/null 2>&1 || true
  docker builder prune -af >/dev/null 2>&1 || true
}

# Build an image FROM the base and export it to an OCI tar (no second store unpack).
#   oci_build_from_base <dockerfile> <image-tag> <out-tar> [extra --build-arg flags…]
# `-t` is REQUIRED with the OCI exporter: it writes io.containerd.image.name into the tar so
# `sbx template load` tags it (else it loads untagged and `sbx run -t <name>` silently uses a STALE
# old image of that name).
oci_build_from_base() {
  local dockerfile="$1" tag="$2" out="$3"; shift 3
  ensure_base_loaded
  local local_ctx; local_ctx="$(new_ctx)"
  stage_ctx_assets "$local_ctx"

  # Expose the base image to the container-driver builder via a short-lived local registry.
  # --driver-opt network=host lets the builder reach localhost:$REG_PORT inside the container.
  log "starting temporary local registry on localhost:${REG_PORT}…"
  docker rm -f "$REG_CNAME" >/dev/null 2>&1 || true
  docker run -d --name "$REG_CNAME" -p "127.0.0.1:${REG_PORT}:5000" registry:2
  local reg_base="localhost:${REG_PORT}/${BASE_TAG}"
  docker tag "$BASE_TAG" "$reg_base"
  docker push "$reg_base"

  if ! docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
    log "creating docker-container builder '$BUILDER_NAME' (supports OCI export)…"
    docker buildx create --name "$BUILDER_NAME" --driver docker-container \
      --driver-opt network=host
  fi

  log "building $tag (FROM $BASE_TAG) -> OCI tar $out  (no image-store unpack)…"
  docker buildx build -f "$dockerfile" "${PROXY_ARGS[@]}" \
    --builder "$BUILDER_NAME" \
    -t "$tag" \
    --build-arg BASE="$reg_base" \
    "$@" \
    --output "type=oci,dest=$out" "$local_ctx"
  rm -rf "$local_ctx"; ls -lh "$out"
  reclaim "$BASE_TAG"
}

case "$TARGET" in
  base)
    build_base
    OUT="$HERE/workspace/nix-direnv-sandbox.tar"
    log "exporting $BASE_TAG -> $OUT …"
    docker save "$BASE_TAG" -o "$OUT"; ls -lh "$OUT"
    reclaim "$BASE_TAG"
    HOSTTAG="${BASE_TAG%%:*}"; OUTFILE="nix-direnv-sandbox.tar"
    ;;
  splice)
    oci_build_from_base "$HERE/docker/splice-ready.Dockerfile" "$SPLICE_TAG" "$HERE/workspace/splice-ready.tar" \
      --build-arg SPLICE_REPO="$SPLICE_REPO" \
      --build-arg SPLICE_REF="$SPLICE_REF"
    HOSTTAG="${SPLICE_TAG%%:*}"; OUTFILE="splice-ready.tar"
    ;;
  daml)
    oci_build_from_base "$HERE/docker/daml-ready.Dockerfile" "$DAML_TAG" "$HERE/workspace/daml-ready.tar" \
      --build-arg DAML_REPO="$DAML_REPO" \
      --build-arg DAML_REF="$DAML_REF" \
      --build-arg BAZEL_FETCH_TARGETS="$BAZEL_FETCH_TARGETS"
    HOSTTAG="${DAML_TAG%%:*}"; OUTFILE="daml-ready.tar"
    ;;
  *) echo "unknown target '$TARGET' (use: base | splice | daml)" >&2; exit 2 ;;
esac

cat <<EOF

[build-template] done. Now, on the HOST:
  sbx template load "$HERE/workspace/$OUTFILE"
  sbx run -t $HOSTTAG --name <new-name> claude <fresh-workspace>

The nix daemon auto-starts at boot, so nix + direnv are ready immediately.
EOF

if [ "$TARGET" = daml ]; then
cat <<'EOF'

daml note: its dev env is in sdk/ but its .git is at the repo ROOT. sbx mounts only the path you
name, so make the REPO ROOT the workspace and cd into sdk inside — never point sbx straight at sdk/
(that leaves .git unmounted: "fatal: not a git repository"). E.g.:
  sbx run -t daml-ready --name <new-name> claude /path/to/daml      # mount the root
  # inside (no direnv needed — the dev-env wrappers self-bootstrap):
  #   cd sdk && eval "$(dev-env/bin/dade assist)" && daml-bazel-prepare --vdx && bazel build //...
  #
  # bazel artifacts won't fit the 20G overlay, so daml-bazel-prepare moves the output base off it:
  #   daml-bazel-prepare          # -> host-mounted disk (roomy, persistent, but slow virtiofs)
  #   daml-bazel-prepare --vdc    # -> local /var/lib/docker disk (fast; sandbox-lifetime; shares the
  #                               #    nested-Docker disk) — use when not running nested Docker
EOF
fi
