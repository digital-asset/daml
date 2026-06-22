#!/usr/bin/env bash
# bootstrap-nix.sh — reusable, idempotent toolchain bootstrap for nix-based projects (e.g. splice).
#
# What it does:
#   1. Installs Nix (Determinate Systems installer, --init none for containers w/o systemd)
#   2. Starts the nix daemon manually (no systemd in this sandbox)
#   3. Enables flakes + nix-command
#   4. Installs direnv + nix-direnv from GitHub nixpkgs (NOT flakehub, which is firewalled)
#   5. Wires nix-direnv + persists the CORE nix env into $CLAUDE_ENV_FILE
#
# Survives sandbox rebuilds by being re-runnable: this script lives in the repo (host-mounted),
# and re-running it deterministically reinstalls the toolchain. (/nix and the running daemon are
# container-only and lost on rebuild — just re-run this script.)
#
# Requires these hosts allowed in the network policy (run on HOST):
#   sbx policy allow network install.determinate.systems,cache.nixos.org,nixos.org,releases.nixos.org,channels.nixos.org,repo1.maven.org
#   (github.com + api/codeload subdomains are already reachable; flakehub.com is NOT — we avoid it.)
set -euo pipefail

PERSIST="${CLAUDE_ENV_FILE:-/etc/sandbox-persistent.sh}"
NIX_DAEMON_SH="/nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh"
NIX_DAEMON_BIN="/nix/var/nix/profiles/default/bin/nix-daemon"
NIX_SOCKET="/nix/var/nix/daemon-socket/socket"
NIXPKGS="github:NixOS/nixpkgs/nixpkgs-unstable"   # source from GitHub, build deps from cache.nixos.org

log() { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }

# Load nix into THIS shell. The Claude Code shell snapshot can reset PATH AFTER the profile script's
# run-once guard fires, leaving nix off PATH — so always unset the guard before sourcing.
load_nix() {
  unset __ETC_PROFILE_NIX_SOURCED || true
  # shellcheck disable=SC1090
  [ -e "$NIX_DAEMON_SH" ] && . "$NIX_DAEMON_SH"
}

# --- 1. Install Nix (idempotent) ---------------------------------------------
if [ -e "$NIX_DAEMON_SH" ]; then
  log "Nix already installed — skipping install."
else
  log "Installing Nix (Determinate Systems installer)…"
  curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix \
    | sh -s -- install linux --no-confirm --init none
fi
load_nix

# --- 2. Start the nix daemon (no systemd here) -------------------------------
if [ -S "$NIX_SOCKET" ]; then
  log "nix daemon socket already present."
else
  log "Starting nix daemon (no systemd; backgrounded)…"
  sudo "$NIX_DAEMON_BIN" >/tmp/nix-daemon.log 2>&1 &
  for _ in $(seq 1 30); do [ -S "$NIX_SOCKET" ] && break; sleep 1; done
  [ -S "$NIX_SOCKET" ] || { echo "daemon socket never appeared; see /tmp/nix-daemon.log" >&2; exit 1; }
fi

# --- 3. Enable flakes + nix-command ------------------------------------------
NIX_CONF_DIR="${HOME}/.config/nix"
mkdir -p "$NIX_CONF_DIR"
if ! grep -qs 'experimental-features' "$NIX_CONF_DIR/nix.conf" 2>/dev/null; then
  log "Enabling flakes + nix-command in $NIX_CONF_DIR/nix.conf"
  echo 'experimental-features = nix-command flakes' >> "$NIX_CONF_DIR/nix.conf"
fi

# --- 4. Install direnv + nix-direnv (from GitHub nixpkgs, not flakehub) ------
if [ -x "$HOME/.nix-profile/bin/direnv" ]; then
  log "direnv already installed — skipping."
else
  log "Installing direnv + nix-direnv from $NIXPKGS …"
  nix profile add "$NIXPKGS#direnv" "$NIXPKGS#nix-direnv"
fi

# --- 5. Wire nix-direnv + persist CORE env -----------------------------------
NIX_DIRENV_RC="$HOME/.nix-profile/share/nix-direnv/direnvrc"
mkdir -p "${HOME}/.config/direnv"
if [ -e "$NIX_DIRENV_RC" ] && ! grep -qsF "$NIX_DIRENV_RC" "${HOME}/.config/direnv/direnvrc" 2>/dev/null; then
  log "Wiring nix-direnv into ~/.config/direnv/direnvrc"
  echo "source $NIX_DIRENV_RC" >> "${HOME}/.config/direnv/direnvrc"
fi

# Persist CORE env (NO completion scripts — they break the Bash tool; see CLAUDE.md).
add_persist() {
  local line="$1"
  grep -qsF "$line" "$PERSIST" 2>/dev/null || echo "$line" | sudo tee -a "$PERSIST" >/dev/null
}
log "Persisting nix env into $PERSIST"
add_persist "[ -e $NIX_DAEMON_SH ] && . $NIX_DAEMON_SH"
add_persist 'export PATH="$HOME/.nix-profile/bin:/nix/var/nix/profiles/default/bin:$PATH"'
# NOTE: no `direnv hook bash` (interactive-only PROMPT_COMMAND) and no daemon auto-start in the
# persistent file — keep it side-effect-free. Use `direnv exec <dir> <cmd>` for non-interactive runs;
# re-run this script after a sandbox rebuild to reinstall nix + restart the daemon.

log "Done. nix: $(nix --version 2>/dev/null), direnv: $($HOME/.nix-profile/bin/direnv --version 2>/dev/null)"
log "NOTE: in THIS shell session nix may not be on PATH yet (stale snapshot). New sessions are fine."
log "      For this session, prefix commands with:  unset __ETC_PROFILE_NIX_SOURCED; . $NIX_DAEMON_SH"
