#!/bin/bash

# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# This script downloads and installs the DAML SDK on Linux and macOS.
# This will overwrite any existing installation in ~/.daml
#

readonly ORIGDIR="$(pwd)"
function cleanup() {
  echo "$(tput setaf 3)FAILED TO INSTALL!$(tput sgr 0)"
  cd $ORIGDIR
}
trap cleanup EXIT

readonly TMPDIR="$(mktemp -d)"
cd $TMPDIR

# Check if curl and tar are available.
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


readonly DAML_HOME="$HOME/.daml"
if [ -d $DAML_HOME ] ; then
  chmod -R u+w $DAML_HOME
  rm -rf $DAML_HOME
fi


echo "Determining latest DAML version..."
readonly VERSION="$(curl -s https://github.com/digital-asset/daml/releases/latest | sed 's/^.*github.com\/digital-asset\/daml\/releases\/tag\/v//' | sed 's/".*$//')"
echo "Latest DAML version is $VERSION."


if [[ "$OSTYPE" == "linux-gnu" ]]; then
  OS="linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS="macos"
else
  echo "Operating system not supported:"
  echo "  OSTYPE = $OSTYPE"
  exit 1
fi

readonly TARBALL="daml-sdk-$VERSION-$OS.tar.gz"
readonly URL="https://github.com/digital-asset/daml/releases/download/v$VERSION/$TARBALL"

echo "$(tput setaf 3)Downloading latest DAML SDK. This may take a while.$(tput sgr 0)"
curl -L $URL --output $TARBALL --progress-bar

echo "Extracting SDK release tarball."
mkdir -p $TMPDIR/sdk
tar xzf $TARBALL -C $TMPDIR/sdk --strip-components 1
$TMPDIR/sdk/install.sh

cd $ORIGDIR
trap - EXIT
echo "$(tput setaf 3)Successfully installed DAML.$(tput sgr 0)"

