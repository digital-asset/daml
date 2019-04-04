#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euxo pipefail

#Run this from the ledger dir

PATH="${PATH}:/usr/sbin" #lsof is not on the PATH by default

WORKING_DIR=$(pwd)
PORT=6865

function get_pid_listening_on_port () {
  echo $(lsof -i:${PORT} -t)
}

function cleanup() {
  PID=$(get_pid_listening_on_port) || "" #Handling error from lsof if there is no such process
  if [ -z ${PID} ]
  then
        exit 0
  else
        kill ${PID}
  fi
}
trap cleanup EXIT

function wait_for_unbind () {
  while nc -z 127.0.0.1 $1; do
    sleep 0.1
  done
}

function wait_for_bind () {
  while ! nc -z 127.0.0.1 $1; do
    sleep 0.1
  done
}

function getTime () {
  TIME_NANOS=$(date +%s%N | sed -E "s/^([0-9]+)N$/\1000000000/g")
  echo $((${TIME_NANOS}/1000000))
}

function run_startup_benchmark () {
  JAR_FILE=$1
  PORT=$2
  START=$(getTime)
  java -jar ${JAR_FILE} -p ${PORT} --dalf bond-trading.dalf > "sandbox_output.txt" &
  PID=$!
  wait_for_bind ${PORT}
  END=$(getTime)
  kill ${PID}
  wait_for_unbind ${PORT}
  let "DIFF = ${END} - ${START}"
  echo ${DIFF}
}

#Run this from the ledger dir
find sandbox -name "sandbox*.tgz" -exec tar -zxf {} +;
TAR_DIR=$(find . -type d -name "sandbox-*-*")
DAMLI_JAR_FILE=$(find ${TAR_DIR} -name "damli-*-*.jar" -maxdepth 2)
java -jar ${DAMLI_JAR_FILE} export-lf-v1 "../reference-apps/bond-trading/daml/LibraryModules.daml" -o "${TAR_DIR}/bond-trading.dalf"
pushd ${TAR_DIR}
JAR_FILE=$(find . -name "sandbox-*-*.jar" -maxdepth 1)
RESULT_FILE_NAME="StartupBenchmark.xml"
RESULT_FILE="${WORKING_DIR}/${RESULT_FILE_NAME}"
echo "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" > ${RESULT_FILE}
echo "<testResults>" >> ${RESULT_FILE}
for i in {1..5}
do
  RESULT_MS=$(run_startup_benchmark "${JAR_FILE}" "${PORT}")
  TIME_MS=$(getTime)
  echo "<sample lb=\"Sandbox.Startup\" lt=\"0\" rc=\"200\" rm=\"OK\" s=\"true\" t=\"${RESULT_MS}\" tn=\"-\" ts=\"${TIME_MS}\"/>" >> ${RESULT_FILE}
done
popd
echo "</testResults>" >> ${RESULT_FILE}
rm -rf ${TAR_DIR}
TARGET_DIR="sandbox-perf/target/benchmarks/jmeter"
mkdir -p ${TARGET_DIR}
mv ${RESULT_FILE} ${TARGET_DIR}
cat "${TARGET_DIR}/${RESULT_FILE_NAME}"

