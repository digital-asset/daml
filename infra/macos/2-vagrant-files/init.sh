#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Note: this script is ran as root on VM creation as per the Vagrantfile.

set -euo pipefail

if [ "$1" = "unset" ] || [ "$2" = "unset" ]; then
    echo "Please set the VSTS_TOKEN and GUEST_NAME env vars before running \`vagrant up\`." >&2
    exit 1
fi

LOGFILE=/Users/vagrant/run.log
touch $LOGFILE
chmod a+w $LOGFILE

log () {
    echo $(/bin/date -u +%Y-%m-%dT%H:%M:%S%z) [$SECONDS] $1 >> $LOGFILE
}

log "Starting init script."

# macOS equivalent of useradd & groupadd
dscl . -create /Users/vsts
dscl . -create /Users/vsts UserShell /bin/bash
dscl . -create /Users/vsts RealName "$2"
# Any number is fine here, but it should not exist already.
USER_ID=3000
if id $USER_ID 2>/dev/null; then
    echo "User ID 3000 already exists. Provisioning should only be ran once; if"
    echo "this is the first time you run it, you need to amend the script."
    exit 1
fi
dscl . -create /Users/vsts UniqueID "$USER_ID"
# 20 is the groupid for the "staff" group, which is the default group for
# non-admin users. Unlike on Linux, on macOS users do not get their own group
# by default.
dscl . -create /Users/vsts PrimaryGroupID 20
dscl . -create /Users/vsts NFSHomeDirectory /Users/vsts
mkdir -p /Users/vsts
chown vsts:staff /Users/vsts
# END: macOS equivalent of useradd & groupadd

log "Done creating user vsts."

# Homebrew must be installed by an admin, and I would rather not make vsts one.
# So we need a little dance here.
su -l vagrant <<'HOMEBREW_INSTALL'
set -euo pipefail
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)" < /dev/null
HOMEBREW_INSTALL
# /usr/local itself belongs to root and cannot be changed, but we can change
# the subdirs
chown -R vsts /usr/local/*

log "Done installing Homebrew."

# Install jq
su -l vsts <<'TOOLS_INSTALL'
set -euo pipefail
cat << BASHRC >> /Users/vsts/.bashrc
if [ -d /usr/local/bin ]; then
    export PATH="/usr/local/bin:\$PATH"
fi
BASHRC
/usr/local/bin/brew install jq netcat
TOOLS_INSTALL

log "Done installing tools through Homebrew."

su -l vsts <<AGENT_SETUP
set -euo pipefail

cat /etc/passwd
echo $SHELL
echo \$SHELL

# somehow the change to ~/.bashrc doesn't get picked up here, though it does
# get picked up when I ssh in and run su.
export PATH="/usr/local/bin:\$PATH"

VSTS_ACCOUNT=digitalasset
VSTS_POOL=macOS-pool
VSTS_TOKEN=$1

mkdir -p ~/agent
cd ~/agent

echo Determining matching VSTS agent...
VSTS_AGENT_RESPONSE=\$(curl -sSfL \
  -u "user:\$VSTS_TOKEN" \
  -H 'Accept:application/json;api-version=3.0-preview' \
  "https://\$VSTS_ACCOUNT.visualstudio.com/_apis/distributedtask/packages/agent?platform=osx-x64")

VSTS_AGENT_URL=\$(echo "\$VSTS_AGENT_RESPONSE" \
  | jq -r '.value | map([.version.major,.version.minor,.version.patch,.downloadUrl]) | sort | .[length-1] | .[3]')

if [ -z "\$VSTS_AGENT_URL" -o "\$VSTS_AGENT_URL" == "null" ]; then
  echo 1>&2 error: could not determine a matching VSTS agent - check that account \\'\$VSTS_ACCOUNT\\' is correct and the token is valid for that account
  exit 1
fi

echo Downloading and installing VSTS agent...
curl -sSfL "\$VSTS_AGENT_URL" | tar -xz --no-same-owner

set +u
source ./env.sh
set -u

./config.sh \
  --acceptTeeEula \
  --agent "$2" \
  --auth PAT \
  --pool "\$VSTS_POOL" \
  --replace \
  --token "\$VSTS_TOKEN" \
  --unattended \
  --url "https://\$VSTS_ACCOUNT.visualstudio.com"
AGENT_SETUP

## Hardening
chown -R root:wheel /Users/vsts/agent/{*.sh,bin,externals}

log "Done installing VSTS agent."

# create /nix partition
hdiutil create -size 20g -fs 'Case-sensitive APFS' -volname Nix -type SPARSE /System/Volumes/Data/Nix.dmg
hdiutil attach /System/Volumes/Data/Nix.dmg.sparseimage -mountpoint /nix

log "Created /nix partition."

# Note: installing Nix in single-user mode with /nix already existing and
# writeable does not require sudoer access
su -l vsts <<'END'
set -euo pipefail
bash <(curl https://nixos.org/nix/install)
echo "build:darwin --disk_cache=~/.bazel-cache" > ~/.bazelrc
END

log "Done installing nix."

# This one is allowed to fail; the goal here is to initialize /nix and
# ~/.bazel-cache. If this fails, we probably still have a useful node, though
# it may be slower on its first job.
su -l vsts <<'END'
cd $(mktemp -d)
git clone https://github.com/digital-asset/daml.git
cd daml
./ci/dev-env-install.sh
./build.sh "_$(uname)"
cd ..
rm -rf daml
exit 0
END

log "Done initializing nix store."

# run the fake local webserver, taken from the docker image
web-server() {
  while true; do
    printf 'HTTP/1.1 302 Found\r\nLocation: https://%s.visualstudio.com/_admin/_AgentPool\r\n\r\n' "digitalasset" | /usr/local/bin/nc -l -p 80 > /dev/null
  done
}
web-server &

log "Started web server."

# Start the VSTS agent
log "Starting agent..."
su -l vsts <<END
cd /Users/vsts/agent
./run.sh >> $LOGFILE &
END
