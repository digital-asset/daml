#!/bin/sh

# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# DAML is an open-source privacy-aware smart contract language.
# This script downloads and installs the DAML SDK on Linux and macOS.
# This will overwrite any existing installation in ~/.daml
# For more information please visit https://daml.com/ and https://docs.daml.com/
#

set -eu
readonly ORIGDIR="$(pwd)"
cleanup() {
  echo "$(tput setaf 3)FAILED TO INSTALL!$(tput sgr 0)"
}
trap cleanup EXIT

readonly TMPDIR="$(mktemp -d)"
cd $TMPDIR

#
# Check if curl and tar are available.
#
if [ -x "$(command -v curl)" ]; then
  MISSING=""
else
  MISSING="curl"
fi
if [ -x "$(command -v tar)" ]; then
  MISSING="$MISSING"
elif [ -n "$MISSING" ]; then
  MISSING="$MISSING, tar"
else
  MISSING="tar"
fi
if [ -n "$MISSING" ]; then
  echo "Missing tools required for DAML installation: $MISSING"
  exit 1
fi

#
# Determine latest SDK version
#
echo "Determining latest SDK version..."
readonly VERSION="$(curl -sS https://github.com/digital-asset/daml/releases/latest | sed 's/^.*github.com\/digital-asset\/daml\/releases\/tag\/v//' | sed 's/".*$//')"
if [ -z "$VERSION" ] ; then
  echo "Failed to determine latest SDK version."
  exit 1
fi
echo "Latest SDK version is $VERSION."

#
# Determine operating system.
#
readonly OSNAME="$(uname -s)"
if [ "$OSNAME" = "Linux" ] ; then
  OS="linux"
elif [ "$OSNAME" = "Darwin" ] ; then
  OS="macos"
else
  echo "Operating system not supported:"
  echo "  OSNAME = $OSNAME"
  exit 1
fi

#
# Download SDK tarball
#
readonly TARBALL="daml-sdk-$VERSION-$OS.tar.gz"
readonly URL="https://github.com/digital-asset/daml/releases/download/v$VERSION/$TARBALL"

echo "$(tput setaf 3)Downloading latest DAML SDK. This may take a while.$(tput sgr 0)"
curl -L $URL --output $TARBALL --progress-bar
if [ ! -f $TARBALL ] ; then
  echo "Failed to download SDK tarball."
  exit 1
fi

#
# Remove existing installation.
#
readonly DAML_HOME="$HOME/.daml"
if [ -d $DAML_HOME ] ; then
  echo "Removing existing installation."
  chmod -R u+w $DAML_HOME
  rm -rf $DAML_HOME
fi

#
# Extract and install SDK tarball.
#
echo "Extracting SDK release tarball."
mkdir -p $TMPDIR/sdk
tar xzf $TARBALL -C $TMPDIR/sdk --strip-components 1
$TMPDIR/sdk/install.sh
if [ ! -d $DAML_HOME ] ; then
  exit 1
fi

#
# Done.
#
trap - EXIT
echo "$(tput setaf 3)Successfully installed DAML.$(tput sgr 0)"

