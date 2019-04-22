#!/usr/bin/env bash

run() {
  echo "$ $*"
  "$@"
}


run env

run pwd

run which bash
run which find
run which awk
run which grep
