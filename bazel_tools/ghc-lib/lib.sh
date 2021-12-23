#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

make_absolue() {
  local P="$1"
  case "$P" in
    /*|[a-zA-Z]:/*|[a-zA-Z]:\\*) echo "$P";;
    *) echo "$PWD/$P";;
  esac
}

make_all_absolute() {
  local IN="$1" ARR P OUT=""
  # TODO[AH] Handle Windows separator
  IFS=':' read -ra ARR <<<"$IN"
  for P in "${ARR[@]}"; do
    OUT="$OUT$(make_absolue "$P"):"
  done
  echo "${OUT%:}"
}

abs_dirname() {
  local IN="$1"
  echo "$(make_absolue "$(dirname "$IN")")"
}
