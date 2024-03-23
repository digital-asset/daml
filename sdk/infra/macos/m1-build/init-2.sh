#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Note: this script is ran as root on VM creation as per the Vagrantfile.

set -euo pipefail

echo "Starting init script."

# macOS equivalent of useradd & groupadd
dscl . -create /Users/vsts
dscl . -create /Users/vsts UserShell /bin/bash
dscl . -create /Users/vsts RealName "Azure Agent"
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

echo "Done creating user vsts."

# Homebrew must be installed by an admin, and I would rather not make vsts one.
# So we need a little dance here.
su -l admin <<'HOMEBREW_INSTALL'
set -euo pipefail
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)" < /dev/null
HOMEBREW_INSTALL
# /usr/local itself belongs to root and cannot be changed, but we can change
# the subdirs
chown -R vsts /opt/homebrew
chown -R vsts /opt/homebrew/*

echo "Done installing Homebrew."

# Install jq
su -l vsts <<'TOOLS_INSTALL'
set -euo pipefail
cat << BASHRC >> /Users/vsts/.bashrc
if [ -d /opt/homebrew/bin ]; then
    export PATH="/opt/homebrew/bin:\$PATH"
fi
BASHRC
/opt/homebrew/bin/brew install jq netcat xz
TOOLS_INSTALL

echo "Done installing tools through Homebrew."

# Install Rosetta 
/usr/sbin/softwareupdate --install-rosetta --agree-to-license

# create /nix partition
hdiutil create -size 60g -fs 'Case-sensitive APFS' -volname Nix -type SPARSE /System/Volumes/Data/Nix.dmg
hdiutil attach /System/Volumes/Data/Nix.dmg.sparseimage -mountpoint /nix

echo "Created /nix partition."

## cache

CACHE_SCRIPT=/Users/vsts/reset_caches.sh

cat <<'RESET_CACHES' > $CACHE_SCRIPT
#!/usr/bin/env bash

set -euo pipefail

reset_cache() {
    local file mount_point
    file=$1
    mount_point=$2

    echo "Cleaning up '$mount_point'..."
    if [ -d "$mount_point" ]; then
        for pid in $(pgrep -a -f bazel | awk '{print $1}'); do
            echo "Killing $pid..."
            kill -s KILL $pid
        done
        for pid in $(lsof $mount_point | sed 1d | awk '{print $2}' | sort -u); do
            echo "Killing $pid..."
            kill -s KILL $pid
        done
        if hdiutil info | grep $mount_point; then
            hdiutil detach "$mount_point"
        fi
        rm -rf $mount_point
    fi

    rm -f "${file}.sparseimage"
    hdiutil create -size 200g -fs APFS -volname "$file" -type SPARSE "$file"
    mkdir -p $mount_point
    hdiutil attach "${file}.sparseimage" -mountpoint "$mount_point"
    echo "Done."
}

reset_cache /var/tmp/bazel_cache.dmg /var/tmp/_bazel_vsts
reset_cache /var/tmp/disk_cache.dmg /Users/vsts/.bazel-cache
RESET_CACHES
chown vsts:staff $CACHE_SCRIPT
chmod +x $CACHE_SCRIPT

su -l vsts <<END
/Users/vsts/reset_caches.sh
END

echo "created cache partitions"

# Note: installing Nix in single-user mode with /nix already existing and
# writeable does not require sudoer access
# Single-user install is no longer available in the 2.4+ installer, so we pin
# _the installer_ to 2.3.16 then upgrade to nix 2.4.x.
su -l vsts <<'END'
set -euo pipefail
export PATH="/usr/sbin:$PATH"
(
cd $(mktemp -d)
curl https://releases.nixos.org/nix/nix-2.10.3/nix-2.10.3-aarch64-darwin.tar.xz > tarball
tar xzf tarball
cd nix-2.10.3-aarch64-darwin
printf '64d\nw\n' | ed -s install
./install --no-daemon
)
source /Users/vsts/.nix-profile/etc/profile.d/nix.sh
nix upgrade-nix
echo "build:darwin --disk_cache=~/.bazel-cache" > ~/.bazelrc
END

echo "Done installing nix."

# This one is allowed to fail; the goal here is to initialize /nix and
# ~/.bazel-cache. If this fails, we probably still have a useful node, though
# it may be slower on its first job.
su -l vsts <<'END'
cd $(mktemp -d)
git clone https://github.com/digital-asset/daml.git
cd daml
#./ci/dev-env-install.sh
#./build.sh "_$(uname)"
cd ..
rm -rf daml
exit 0
END

echo "Done initializing nix store & Bazel cache."
echo "Machine setup complete; shutting down."

#shutdown -h now
exit 0
