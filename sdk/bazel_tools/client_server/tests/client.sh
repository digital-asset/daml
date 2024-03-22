#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

foobar=0
port=""
inputFile=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --target-port) port="$2"; shift 2;;
    --target-port=*) port="${1#*=}"; shift 1;;
    --foobar) foobar=1; shift 1;;
    -*) echo "unknown option: $1" >&2; exit 1;;
    *) inputFile="$1"; shift 1;;
  esac
done

if [ "$foobar" -ne 1 ]; then
  echo "--foobar not passed in"
  exit 1
fi

if [[ -z "$port" || -z "$inputFile" ]]; then
  echo "target port or input file not specified"
  exit 1
fi


exec 5<> /dev/tcp/localhost/$port
cat $inputFile >&5
cat <&5

# Write the output file for testing client_server_build.
if [ -n "${CS_OUT-}" ]; then
  echo "hello" > $CS_OUT
fi
