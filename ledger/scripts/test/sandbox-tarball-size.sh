#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euxo pipefail

#Run this from the ledger dir

WORKING_DIR=$(pwd)
LIMIT_BYTES=$1

function getTime () {
  TIME_NANOS=$(date +%s%N | sed -E "s/^([0-9]+)N$/\1000000000/g")
  echo $((${TIME_NANOS}/1000000))
}

function isSuccess () {
  if [ "${LIMIT_BYTES}" -lt "$1" ]
  then
        echo "\"false\""
  else
        echo "\"true\""
  fi
}


SIZE=`find sandbox -name "sandbox*.tgz" -exec wc -c {} \; | awk '{print $1}'`
RESULT_FILE_NAME="TarBallSize.xml"
RESULT_FILE="${WORKING_DIR}/${RESULT_FILE_NAME}"
echo "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" > ${RESULT_FILE}
echo "<testResults>" >> ${RESULT_FILE}
  TIME_MS=$(getTime)
  echo "<sample lb=\"Sandbox.Tar.Size\" lt=\"0\" rc=\"200\" rm=\"OK\" s=$(isSuccess ${SIZE}) t=\"${SIZE}\" tn=\"-\" ts=\"${TIME_MS}\"/>" >> ${RESULT_FILE}
echo "</testResults>" >> ${RESULT_FILE}
TARGET_DIR="sandbox-perf/target/benchmarks/jmeter"
mkdir -p ${TARGET_DIR}
mv ${RESULT_FILE} ${TARGET_DIR}

