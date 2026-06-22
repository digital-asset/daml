# syntax=docker/dockerfile:1
#
# nix-direnv-sandbox — a GENERIC, shareable sbx template. It is the claude-code sandbox base plus a
# working Nix + direnv toolchain: Determinate Nix, flakes/nix-command enabled, direnv + nix-direnv in
# the `agent` profile, and the nix daemon auto-started at boot. Point it at ANY nix repo (drop a
# `use flake` .envrc) and it just works.
#
# Deliberately contains NOTHING project-specific or private — its build only touches
# cache.nixos.org, install.determinate.systems and github. Safe to publish to a public registry.
# (The private `splice-ready` template builds FROM this and adds splice's closure on top.)
#
# Build:  scripts/build-template.sh base    ->  workspace/nix-direnv-sandbox.tar
# Host:   sbx template load workspace/nix-direnv-sandbox.tar
#         sbx run -t nix-direnv-sandbox claude <workspace>

ARG BASE=docker.io/docker/sandbox-templates:claude-code-docker
FROM ${BASE}

# Build-time proxy (CONNECT tunnel, so no MITM CA needed). Not baked into the runtime image.
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ARG NO_PROXY=
ENV HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY} \
    http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} no_proxy=${NO_PROXY}

ARG NIXPKGS=github:NixOS/nixpkgs/nixpkgs-unstable

USER root
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN <<'BASH'
set -euxo pipefail

# 1. Install Determinate Nix (--init none: no systemd in a build layer) + enable flakes.
curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix \
  | sh -s -- install linux --no-confirm --init none
. /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh
mkdir -p /etc/nix
grep -qs 'experimental-features' /etc/nix/nix.conf || \
  echo 'experimental-features = nix-command flakes' >> /etc/nix/nix.conf

# 2. Daemon up for the duration of this layer.
/nix/var/nix/profiles/default/bin/nix-daemon >/tmp/nix-daemon.log 2>&1 &
dpid=$!
for _ in $(seq 1 60); do [ -S /nix/var/nix/daemon-socket/socket ] && break; sleep 1; done
[ -S /nix/var/nix/daemon-socket/socket ] || { cat /tmp/nix-daemon.log; exit 1; }

# 3. Install direnv + nix-direnv (+ lnav) into the `agent` profile (runtime user) and wire direnvrc.
#    flakehub.com is firewalled, so source from GitHub nixpkgs.
#    lnav is a generic log viewer baked here so it's on PATH in EVERY sandbox without activating any
#    project dev shell — `sbx exec -it <name> bash` then `lnav log/` just works. (splice ships lnav
#    only inside its dev shell, and via an x86_64-musl pin that won't run on aarch64; the nixpkgs
#    build here is architecture-correct.)
runuser -u agent -- env HOME=/home/agent PATH=/nix/var/nix/profiles/default/bin:/usr/bin:/bin \
  nix profile install "$NIXPKGS#direnv" "$NIXPKGS#nix-direnv" "$NIXPKGS#lnav"
runuser -u agent -- bash -c '
  mkdir -p ~/.config/direnv
  rc="$HOME/.nix-profile/share/nix-direnv/direnvrc"
  [ -e "$rc" ] && ! grep -qsF "$rc" ~/.config/direnv/direnvrc 2>/dev/null && echo "source $rc" >> ~/.config/direnv/direnvrc
'

# 4. Daemon down so the committed layer has no running process / stale socket / root-owned log.
kill "$dpid" 2>/dev/null || true
wait "$dpid" 2>/dev/null || true
rm -f /nix/var/nix/daemon-socket/socket /tmp/nix-daemon.log
BASH

# Persist the core nix env so all shells find nix. NO completion scripts — they break the Bash tool
# when sourced per-command (see CLAUDE.md).
RUN <<'BASH'
set -euo pipefail
P=/etc/sandbox-persistent.sh
touch "$P"
grep -qsF 'profile.d/nix-daemon.sh' "$P" || \
  echo '[ -e /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh ] && . /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh' >> "$P"
grep -qsF '.nix-profile/bin' "$P" || \
  echo 'export PATH="$HOME/.nix-profile/bin:/nix/var/nix/profiles/default/bin:$PATH"' >> "$P"
BASH

# Auto-start the nix daemon at container boot. Multi-user nix needs the daemon for any real work,
# and the persistent env file can't start it safely (it's sourced per-command). So wrap the
# entrypoint: start the daemon once (best-effort, via passwordless sudo) then exec the real command.
RUN <<'BASH'
set -euo pipefail
cat > /usr/local/bin/nix-daemon-entrypoint <<'EOF'
#!/usr/bin/env bash
# Start the multi-user nix daemon once, then hand off to the real command. This runs as `agent`, so
# the daemon (root) is started via passwordless sudo — and the log redirect must be done BY root
# (an agent-performed `>` onto a root-owned file fails), hence the redirect lives inside `bash -c`.
if [ -x /nix/var/nix/profiles/default/bin/nix-daemon ] && [ ! -S /nix/var/nix/daemon-socket/socket ]; then
  sudo -n bash -c 'nohup /nix/var/nix/profiles/default/bin/nix-daemon >>/var/log/nix-daemon.log 2>&1 &' || true
fi
exec "$@"
EOF
chmod +x /usr/local/bin/nix-daemon-entrypoint
BASH

# Don't bake the build-time proxy into the runtime image (each sandbox sets its own).
ENV HTTP_PROXY= HTTPS_PROXY= NO_PROXY= http_proxy= https_proxy= no_proxy=

# Boot as the base image's user/dir (the claude CLI lives under /home/agent), with the daemon wrapper
# in front of the original entrypoint/command.
USER agent
WORKDIR /home/agent/workspace
ENTRYPOINT ["tini", "--", "/usr/local/bin/nix-daemon-entrypoint"]
CMD ["claude", "--dangerously-skip-permissions"]
