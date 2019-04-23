#!/usr/bin/env bash

run() {
  echo "$ $*"
  "$@"
}

systeminfo | findstr Build

exit 1

run env

run pwd

run which bash
run which find
run which awk
run which grep
