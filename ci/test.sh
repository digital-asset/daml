#!/usr/bin/env bash

run() {
  echo "$ $*"
  "$@"
}

systeminfo | findstr Build

run fsutil fsinfo drives

run wmic diskdrive

exit 1

run env

run pwd

run which bash
run which find
run which awk
run which grep
