#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

su -l vsts <<AGENT_SETUP
set -euo pipefail

export PATH="/usr/local/bin:\$PATH"

VSTS_ACCOUNT=digitalasset
VSTS_POOL=macOS-pool
VSTS_TOKEN=$1

mkdir -p ~/agent
cd ~/agent
echo 'assignment=default' > .capabilities

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

## Remount Nix partition
hdiutil attach /System/Volumes/Data/Nix.dmg.sparseimage -mountpoint /nix

su -l vsts <<END
hdiutil attach /var/tmp/bazel_cache.dmg.sparseimage -mountpoint /var/tmp/_bazel_vsts
hdiutil attach /var/tmp/disk_cache.dmg.sparseimage -mountpoint /Users/vsts/.bazel-cache
END

## Hardening
chown -R root:wheel /Users/vsts/agent/{*.sh,bin,externals}

log "Done installing VSTS agent."

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
