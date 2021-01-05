#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
su -l vagrant <<'HOMEBREW_INSTALL'
set -euo pipefail
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)" < /dev/null
HOMEBREW_INSTALL
# /usr/local itself belongs to root and cannot be changed, but we can change
# the subdirs
chown -R vsts /usr/local/*

echo "Done installing Homebrew."

# Install jq
su -l vsts <<'TOOLS_INSTALL'
set -euo pipefail
cat << BASHRC >> /Users/vsts/.bashrc
if [ -d /usr/local/bin ]; then
    export PATH="/usr/local/bin:\$PATH"
fi
BASHRC
/usr/local/bin/brew install jq netcat xz
TOOLS_INSTALL

echo "Done installing tools through Homebrew."

# create /nix partition
hdiutil create -size 20g -fs 'Case-sensitive APFS' -volname Nix -type SPARSE /System/Volumes/Data/Nix.dmg
hdiutil attach /System/Volumes/Data/Nix.dmg.sparseimage -mountpoint /nix

echo "Created /nix partition."

# Note: installing Nix in single-user mode with /nix already existing and
# writeable does not require sudoer access
su -l vsts <<'END'
set -euo pipefail
export PATH="/usr/local/bin:/usr/sbin:$PATH"
bash <(curl -sSfL https://nixos.org/nix/install)
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
./ci/dev-env-install.sh
./build.sh "_$(uname)"
cd ..
rm -rf daml
exit 0
END

echo "Done initializing nix store & Bazel cache."
echo "Machine setup complete; shutting down."

shutdown -h now
