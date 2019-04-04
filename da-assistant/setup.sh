#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o pipefail

# This script aims to install for the first time or upgrade an existing version
# of the DA CLI tool (da). This script is assumed to be packaged in an `arx`
# archive together with the binary distribution and copies the binary
# distribution (binary and dynamic libraries) to ~/.da/cli/<version>/ and then
# symlinks the da binary to ~/.da/bin. It also attempts to add da to the global
# path by symlinking in /usr/local/bin. Once this is done, it runs `da setup` to
# perform the steps necessary to finalise the installation or upgrade.

SHOULD_ADD_PATH=false
ADDED_PATH=false
# Get version from the tool for consistency; take first line and replace spaces.
VERSION=$(da/da --version | sed -n 1p | sed -e 's/\(Installed.*:\) \(.*\)/\2/g' |  sed -e 's/ /-/g')

GLOBAL_BIN_DIR="/usr/local/bin"
DA_DIR="$HOME/.da"
BIN_DIR="$DA_DIR/bin"
TOOL_DIR_NAME="cli"
VERSION_DIR="$DA_DIR/$TOOL_DIR_NAME/$VERSION"

# Whether we're doing a new installation or upgrading, we create the appropriate
# dir and copy the binary distribution (binary + dynamic libraries) there.
mkdir -p "$VERSION_DIR"
cp -af da/* "$VERSION_DIR"

# Ensure $BIN_DIR exists and point the bin/da wrapper to the specific version.
# We do this using a wrapper instead of symlinks due to the relative dynamic
# linker dependencies.
mkdir -p "$BIN_DIR"
cat > "$BIN_DIR/da" << EOF
#!/usr/bin/env sh
exec "$VERSION_DIR/da" "\$@"
EOF
chmod +x "$BIN_DIR/da"

# We prefer that `da` is on the path. Thus, we first check if it's installed. If
# not, we try to set a symlink in $GLOBAL_BIN_DIR if we can, otherwise we
# remember to print a message at the end of the installation/upgrade. Note that
# we don't check whether an existing `da` command actually refers to the DA CLI
# or some other utility.
if ! command -pv da >/dev/null 2>&1 ; then
  SHOULD_ADD_PATH=true
  # Can we write to $GLOBAL_BIN_DIR?
  if test -w $GLOBAL_BIN_DIR && ! test -e "$GLOBAL_BIN_DIR/da" ; then
    ln -ns "$BIN_DIR/da" $GLOBAL_BIN_DIR/da
    ADDED_PATH=true
  fi
fi
test -n "$DA_TESTING" && SHOULD_ADD_PATH=false

if test "$SHOULD_ADD_PATH" = true ; then
  if test "$ADDED_PATH" = true ; then
    echo "Added symlink $GLOBAL_BIN_DIR/da -> $BIN_DIR/da"
  else
    # Print message that the user should manually add `da` to their path
    echo "Please add $BIN_DIR to your PATH."
    echo "Alternatively, add a symlink from a folder already in your path. For example:"
    echo
    echo "  ln -ns \"$BIN_DIR/da\" /usr/local/bin/da"
    echo
  fi
fi

echo "SDK Assistant $VERSION installed."
echo "Run 'da setup' to start using the SDK Assistant."
