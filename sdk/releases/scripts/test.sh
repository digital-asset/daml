#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Script to test a new SDK release.

# Note:
# This script would test the navigator via browserstack. The test is however outdated and needs to
# be ported to DAML 1.2. It's therefore disabled.

set -e

exp_num_of_params=5
if [ "$#" -ne $exp_num_of_params ]; then
  echo "[ERROR] Release testing failed!"
  echo "[ERROR] Illegal number of arguments. (Expected $exp_num_of_params, got $#)"
  echo "[HELP] $0 prepopulated_home_tarball sdk_version da_assistant_binary jar_binary navigator_test_jar"
  exit 1
fi

# The "home" directory, with prepopulated .da for this version.
readonly PREPOP_HOME=$1

# The SDK version we're testing.
readonly VERSION=$2

# Path to the SDK Assistant binary
readonly DA=`pwd`"/$3"

# Path to jar
readonly JAR=`pwd`"/$4"

# Path to navigatortest.jar
readonly NAVIGATORTEST=`pwd`"/$5"

# browserstack user
#readonly BROWSERSTACK_CREDENTIALS_USR=$4

# browserstack password
#readonly BROWSERSTACK_CREDENTIALS_PSW=$6

set -u

# Create a temporary directory for the home directory,
# as tests create temporary files in .da (e.g. .da/proc)
TESTHOME="$(mktemp -d)"
function cleanup() {
  rm -rf "$TESTHOME"
}
trap cleanup EXIT

# Unpack the prepared test home tarball to the temporary directory
tar xzf $PREPOP_HOME -C $TESTHOME --strip-components 1

# Allow use of some of the user's dot directories for improved caching.
# TODO(JM): This saves couple of minutes (outside AWS) and fair bit
# of network traffic, but might still not be a good trade-off...
#for dir in .ivy2 .m2 .sbt; do
  #test -e "$HOME/$dir" && ln -s "$HOME/$dir" "$TESTHOME/$dir"
#done

# Create some wrappers
mkdir -p $TESTHOME/bin
printf "#!/bin/sh\nexit 0" > $TESTHOME/bin/open
chmod +x $TESTHOME/bin/open
ln -sf open $TESTHOME/bin/xdg-open

# Make SDK Assistant available in the expected location
mkdir -p $TESTHOME/.da/bin
ln -s $DA $TESTHOME/.da/bin/da

# Running the Navigator integration tests
(
  #OLD_HOME=$HOME
  export HOME="$TESTHOME"
  export PATH="$TESTHOME/bin:$TESTHOME/.da/bin:$PATH:/bin:/usr/bin"
  export SDK_VERSION="$VERSION"
  export DA_TESTING="1"

  cd $TESTHOME

  #BSUSER="--browserstack-user ${BROWSERSTACK_CREDENTIALS_USR}"
  #BSKEY="--browserstack-key ${BROWSERSTACK_CREDENTIALS_PSW}"

  $DA new getting-started navigatortest_project
  mkdir -p navigatortest_project/daml
  cd navigatortest_project/daml
  rm -f *.daml
  # Set up the daml model packaged with the test
  $JAR xf $NAVIGATORTEST Main.daml
  cd ..

  cat > da.yaml <<EOF
project:
  sdk-version: 0.0.0
  scenario: RightOfUse:example
  name: test
  source: daml/RightOfUse.daml
  parties:
  - Scrooge_McDuck
  - Operator
version: 2
EOF

  $DA stop
  NAVIGATOR_PORT=$($DA start 2>&1 | grep "web browser on" | tr ':' '\n' | tail -1)
  NAVIGATOR_PORT="--navigator-port $NAVIGATOR_PORT"
  # We need the original environment to be able to use nix packages within the tests
  #(
    #export HOME="$OLD_HOME"
    # navigatortest needs to be ported to 1.2
    #java -jar $NAVIGATORTEST $BSUSER $BSKEY $NAVIGATOR_PORT
  #)
  $DA stop
)
