#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

is_windows() {
  case "$OSTYPE" in
    win*) return 0;;
    msys*) return 0;;
    cygwin*) return 0;;
    *) return 1;;
  esac
}

path_list_separtor() {
  # Use ':' even on Windows because msys2 will automatically convert such
  # path lists to the Windows format, using ';' as separator and 'C:\'
  # syntax. Be sure that absolute paths use the format '/c/...' instead of
  # 'C:\...'. See https://www.msys2.org/docs/filesystem-paths/.
  if is_windows; then
    echo ":"
  else
    echo ":"
  fi
}

make_absolute() {
  local P="$1"
  case "$P" in
    /*|[a-zA-Z]:/*|[a-zA-Z]:\\*) echo "$P";;
    *) echo "$PWD/$P";;
  esac
}

make_all_absolute() {
  local IN="$1" ARR P OUT=""
  local SEP="$(path_list_separtor)"
  IFS="$SEP" read -ra ARR <<<"$IN"
  for P in "${ARR[@]}"; do
    OUT="$OUT$(make_absolute "$P")$SEP"
  done
  echo "${OUT%$SEP}"
}

abs_dirname() {
  local IN="$1"
  echo "$(make_absolute "$(dirname "$IN")")"
}
