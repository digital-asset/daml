# syntax=docker/dockerfile:1
#
# splice-ready — the PRIVATE template: the generic `nix-direnv-sandbox` base with splice's PINNED
# OSS nix dev-shell closure baked into /nix as read-only layers. All splice/Canton/DA-specific
# content lives ONLY in this layer; the public base (nix-direnv-sandbox) has none of it.
#
# Because /nix rides in read-only image layers it never touches the 20G writable overlay, so the
# sandbox's small root disk is irrelevant.
#
# Build:  scripts/build-template.sh splice   ->  workspace/splice-ready.tar   (builds the base first)
# Host:   sbx template load workspace/splice-ready.tar && sbx run -t splice-ready claude <ws>
#
# The store is realized with the EXACT command splice's own .envrc uses
# (`nix print-dev-env … path:nix#oss`), so it is byte-identical to a normal splice OSS build.

ARG BASE=nix-direnv-sandbox:latest
FROM ${BASE}

# Build-time proxy (CONNECT tunnel → no MITM CA needed). Not baked into the runtime image.
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ARG NO_PROXY=
ENV HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY} \
    http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} no_proxy=${NO_PROXY}

# Which splice commit to bake. flake.lock + nix/*-sources.json under this ref pin everything.
# Full 40-char SHA — `git fetch origin <ref>` rejects an abbreviated SHA ("couldn't find remote ref").
ARG SPLICE_REPO=https://github.com/canton-network/splice
ARG SPLICE_REF=d1d5dc53951e893deabdddeb38533fe2841f3cfb

USER root
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN <<'BASH'
set -euxo pipefail

# Nix + flakes are already installed by the base; just bring the daemon up for this layer.
. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
/nix/var/nix/profiles/default/bin/nix-daemon >/tmp/nix-daemon.log 2>&1 &
dpid=$!
for _ in $(seq 1 60); do [ -S /nix/var/nix/daemon-socket/socket ] && break; sleep 1; done
[ -S /nix/var/nix/daemon-socket/socket ] || { cat /tmp/nix-daemon.log; exit 1; }

# Clone splice at the pinned ref and realize the OSS dev-shell closure the way splice's .envrc does.
# --profile roots it as a gcroot so it survives `nix store gc` and stays baked in the image.
git init -q /opt/splice
git -C /opt/splice remote add origin "$SPLICE_REPO"
git -C /opt/splice fetch -q --depth 1 origin "$SPLICE_REF"
git -C /opt/splice -c advice.detachedHead=false checkout -q FETCH_HEAD
( cd /opt/splice && nix print-dev-env \
    --profile /nix/var/nix/profiles/splice-oss "path:nix#oss" >/dev/null )
rm -rf /opt/splice

kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket
BASH

# Record the baked splice ref so tooling can detect when a sandbox's cloned checkout has drifted from
# the template's warm nix cache (scripts/sbx-clone.sh reads this to warn about a slower first build).
# Drift is not an error — the dev shell self-heals by realizing the delta from the nix caches.
RUN echo "$SPLICE_REF" > /etc/splice-ready.ref

# Bake splice build guidance into the runtime user's memory (~/.claude/CLAUDE.md). Claude Code loads
# this file regardless of cwd, so EVERY splice-ready sandbox gets it — crucially including
# `sbx run --clone`, where the splice repo is cloned fresh into the container and carries no project
# CLAUDE.md from sbx-templates. The repo itself is never modified. Host clone mode ALSO gets this
# (its project CLAUDE.md under splice-clones/ adds only the host-visible `.build-logs` convention on
# top). build-template.sh stages docker/splice-ready-claude.md into the build context for this COPY.
COPY --chown=agent:agent splice-ready-claude.md /home/agent/.claude/CLAUDE.md

# Don't bake the build-time proxy into the runtime image.
ENV HTTP_PROXY= HTTPS_PROXY= NO_PROXY= http_proxy= https_proxy= no_proxy=

# Restore the runtime user/dir (entrypoint + cmd are inherited from the base).
USER agent
WORKDIR /home/agent/workspace
