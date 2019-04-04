#!/usr/bin/env bash
# Copyright (c) 2019, Digital Asset (Switzerland) GmbH and/or its affiliates.
# All rights reserved.

for actual in $(find . -type f -name "ACTUAL.*"); do
  expected="$(dirname "$actual")/EXPECTED.${actual##*.}"
  diff -u "$actual" "$expected"
  if [ $? -ne 0 ]; then
    echo
    while :; do
      echo -n "overwrite (y/n)? "
      read answer
      if [ "$answer" == "y" ]; then
        cp -v "$actual" "$expected"
        break
      elif [ "$answer" == "n" ]; then
        echo "skipping $actual"
        break
      fi
    done
  fi
done
