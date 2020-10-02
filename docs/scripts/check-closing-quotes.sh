#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Check for invalid quote usage such as "variable `x``"

DIR="$1"
WHITELIST="$2"

if [[ $# -ne 2 ]]
then
  echo "Usage: $0 directory whitelist"
  exit 1
fi

if ! test -f "$WHITELIST"; then
  echo ERROR: Whitelist file not found: "$WHITELIST"
  exit 1
fi

ERRORS=0
REGEX='^(?!([^`]*(((`[^`]*`)|(``[^`]*``))[^`]*)*)$$)'

while read FILE; do
  if grep --invert-match --file="$WHITELIST" "$FILE" | grep --quiet --perl-regexp "$REGEX"; then
    echo Quotation error in "\`$FILE'":
    grep --perl-regexp "$REGEX" "$FILE"
    echo
    ERRORS=$((ERRORS+1))
  fi
done <<< $(find "$DIR" -name '*.rst')

if [ $ERRORS -gt 0 ]; then
  echo "ERROR: $ERRORS file(s) found with errors, see above."
  echo "You can add lines to \`$WHITELIST' to ignore false positives."
  exit 1
fi
