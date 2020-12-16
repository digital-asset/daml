#!/bin/sh

# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# DAML is an open-source privacy-aware smart contract language.
# This script downloads and installs the SDK on Linux and macOS.
# This will overwrite any existing installation in ~/.daml
# For more information please visit https://daml.com/ and https://docs.daml.com/
#

#
# USAGE:
#    get-daml.sh            Download and install the latest SDK release.
#    get-daml.sh VERSION    Download and install given version of SDK.
#

set -eu
readonly SWD="$PWD"
readonly INSTALL_MINSIZE=1000000
if [ -z $TEMPDIR ]; then
  readonly TMPDIR="$(mktemp -d)"
else
  readonly TMPDIR=$TEMPDIR
fi
cd $TMPDIR

# Don't remove user specified temporary directory on cleanup.
rmTmpDir() {
  if [ -z $TEMPDIR ]; then
    rm -rf $TMPDIR
  else
    echo "You may now remove the DAML installation files from $TEMPDIR"
  fi
}

cleanup() {
  echo "$(tput setaf 3)FAILED TO INSTALL!$(tput sgr 0)"
  cd $SWD
  rmTmpDir
}
trap cleanup EXIT


#
# Check that the temporary directory has enough space for the installation
#
if [ "$(df $TMPDIR | tail -1 | awk '{print $4}')" -lt $INSTALL_MINSIZE ]; then
    echo "Not enough disk space available to extract DAML SDK in $TMPDIR."
    echo ""
    echo "You can specify an alternative extraction directory by"
    echo "setting the TEMPDIR environment variable."
    exit 1
fi


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
# Determine SDK version
#
if [ -z "${1:-}" ] ; then
  echo "Determining latest SDK version..."
  readonly VERSION="$(curl -sS https://github.com/digital-asset/daml/releases/latest | sed 's/^.*github.com\/digital-asset\/daml\/releases\/tag\/v//' | sed 's/".*$//')"
  if [ -z "$VERSION" ] ; then
    echo "Failed to determine latest SDK version."
    exit 1
  fi
  echo "Latest SDK version is $VERSION"
else
  readonly VERSION="$1"
fi

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

echo "$(tput setaf 3)Downloading SDK $VERSION. This may take a while.$(tput sgr 0)"
curl -SLf $URL --output $TARBALL --progress-bar
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
cd $SWD
rmTmpDir
