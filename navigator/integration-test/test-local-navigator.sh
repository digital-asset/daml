#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -u

exp_num_of_params=2
if [ "$#" -ne $exp_num_of_params ]; then
  echo "[ERROR] Illegal number of arguments. (Expected $exp_num_of_params, got $#)"
  echo "[HELP] $0 browserstack_user browserstack_password"
  exit 1
fi

# browserstack user
readonly BROWSERSTACK_CREDENTIALS_USR=$1

# browserstack password
readonly BROWSERSTACK_CREDENTIALS_PSW=$2

lsof -n -i :4000 | grep LISTEN | awk '{print $2}' | xargs kill -9
lsof -n -i :7500 | grep LISTEN | awk '{print $2}' | xargs kill -9
lsof -n -i :8081 | grep LISTEN | awk '{print $2}' | xargs kill -9

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR


BSUSER="--browserstack-user ${BROWSERSTACK_CREDENTIALS_USR}"
BSKEY="--browserstack-key ${BROWSERSTACK_CREDENTIALS_PSW}"

DAML_PATH="--daml-path $DIR/src/main/resources/Main.daml"
NAV_CONF_PATH="--nav-conf-path $DIR/src/main/resources/ui-backend.conf"
PARENTDIR="$(dirname "$DIR")"
NAVIGATOR_DIR="--navigator-dir $PARENTDIR"


PARAMS="$DAML_PATH $NAV_CONF_PATH $NAVIGATOR_DIR $BSUSER $BSKEY"


TEST_COMMAND='sbt "run '${PARAMS}'"'

echo ${TEST_COMMAND}

echo "Running the integration tests"
eval "${TEST_COMMAND}"
SUCCESS=$(echo $?)
echo "Test run finished"
echo "Cleaning up the applications"

lsof -n -i :4000 | grep LISTEN | awk '{print $2}' | xargs kill -9
lsof -n -i :7500 | grep LISTEN | awk '{print $2}' | xargs kill -9
lsof -n -i :8081 | grep LISTEN | awk '{print $2}' | xargs kill -9

echo "Cleanup done"
popd

exit ${SUCCESS}
