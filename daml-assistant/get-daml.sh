#!/bin/sh

# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Daml is an open-source privacy-aware smart contract language.
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
readonly INSTALL_MINSIZE=1000000
if [ -z "${TEMPDIR:-}" ]; then
  readonly TMPDIR="$(mktemp -d)"
else
  readonly TMPDIR=$TEMPDIR
  if [ ! -d "$TEMPDIR" ] ; then
    mkdir $TEMPDIR
  fi
fi

# Don't remove user specified temporary directory on cleanup.
rmTmpDir() {
  if [ -z "${TEMPDIR:-}" ]; then
    rm -rf $TMPDIR
  else
    echo "You may now remove the Daml installation files from $TEMPDIR"
  fi
}

cleanup() {
  echo "$(tput setaf 3)FAILED TO INSTALL!$(tput sgr 0)"
  rmTmpDir
}
trap cleanup EXIT


#
# Check that the temporary directory has enough space for the installation
#
if [ -x "$(command -v df)" -a -x "$(command -v awk)" ]; then
  if [ "$(df $TMPDIR | tail -1 | awk '{print $4}')" -lt "$INSTALL_MINSIZE" ]; then
    echo "Not enough disk space available to extract Daml SDK in $TMPDIR."
    echo ""
    echo "You can specify an alternative extraction directory by"
    echo "setting the TEMPDIR environment variable."
    exit 1
  fi
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
  echo "Missing tools required for Daml installation: $MISSING"
  exit 1
fi

#
# Determine SDK version
#
if [ -z "${1:-}" ] ; then
  echo "Determining latest SDK version..."
  readonly VERSION="$(curl -sS https://docs.daml.com/latest)"
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
# Can't assume jq
release_id=$(curl --silent \
                  --location \
                  https://api.github.com/repos/digital-asset/daml/releases/tags/v$VERSION \
              | grep '"id":' \
              | head -1 \
              | sed 's|.*: \([0-9]*\),|\1|')
URL=$(curl --silent \
           --location \
           https://api.github.com/repos/digital-asset/daml/releases/$release_id/assets \
       | grep browser_download_url \
       | grep "daml-sdk-.*-$OS.tar.gz\"" \
       | sed 's|.*: "\(https://[^"]*\)"|\1|')
readonly TARBALL=$(basename $URL)
sdk_version=$(echo $TARBALL | sed 's|daml-sdk-\(.*\)-[^-]*.tar.gz|\1|')

echo "$(tput setaf 3)Downloading SDK $sdk_version for Daml $VERSION. This may take a while.$(tput sgr 0)"
curl -SLf $URL --output $TMPDIR/$TARBALL --progress-bar
if [ ! -f $TMPDIR/$TARBALL ] ; then
  echo "Failed to download SDK tarball."
  exit 1
fi

#
# Remove existing installation.
#
readonly DAML_HOME="$HOME/.daml"
if [ -d $DAML_HOME ] ; then
  echo "Removing existing installation: $DAML_HOME"
  chmod -R u+w $DAML_HOME
  rm -rf $DAML_HOME
fi

#
# Remove existing cache.
#
readonly DAML_CACHE="${XDG_CACHE_HOME:-$HOME/.cache}/daml"
if [ -d $DAML_CACHE ] ; then
  echo "Removing existing cache: $DAML_CACHE"
  rm -rf $DAML_CACHE
fi

#
# Extract and install SDK tarball.
#
echo "Extracting SDK release tarball."
mkdir -p $TMPDIR/sdk
tar xzf $TMPDIR/$TARBALL -C $TMPDIR/sdk --strip-components 1
$TMPDIR/sdk/install.sh
if [ ! -d $DAML_HOME ] ; then
  exit 1
fi

#
# Done.
#
trap - EXIT
echo "$(tput setaf 3)Successfully installed Daml.$(tput sgr 0)"
rmTmpDir
