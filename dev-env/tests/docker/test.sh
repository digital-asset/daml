#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

# This is a smoke test of dev-env installation and execution on Ubuntu. It
# tests:
# - if Nix can be installed in an pristine Ubuntu container
# - it can run 'dade info' command via 'dade-env'
#
# This test file is meant to be executed in a Docker container as
# defined in the Makefile next to this file. Among other things, it
# requires a 'da' checkout to be bind mounted at /code.

# Get required tools.
apt-get -y update
apt-get -y install sudo curl

# Setup an ordinary user with nopasswd sudo access.
useradd tester -m -s /bin/bash
usermod -a -G sudo tester
echo "%sudo ALL=NOPASSWD:ALL" > /etc/sudoers.d/sudo
chmod 0440 /etc/sudoers.d/sudo

if [ $# -eq 1 ]; then
    NIX_INSTALL_ARGS="-f $1"
fi

# Install default Nix.
sudo -i -u tester /code/dev-env/bin/dade-nix-install $NIX_INSTALL_ARGS

# Run 'dade-info' in a dev-env environment.
sudo -i -u tester /code/dev-env/bin/dade-env dade info
