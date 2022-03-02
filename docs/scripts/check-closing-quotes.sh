#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Check for invalid quote usage such as "variable `x``"

DIR="$1"
ALLOWLIST="$2"

if [[ $# -ne 2 ]]
then
  echo "Usage: $0 directory allow-list"
  exit 1
fi

if ! test -f "$ALLOWLIST"; then
  echo ERROR: Allow-list file not found: "$ALLOWLIST"
  exit 1
fi

ERRORS=0
REGEX='^(?!([^`]*(((`[^`]*`)|(``[^`]*``))[^`]*)*)$$)'

while read FILE; do
  if grep --invert-match --file="$ALLOWLIST" "$FILE" | grep --quiet --perl-regexp "$REGEX"; then
    echo Quotation error in "\`$FILE'":
    grep --invert-match --file="$ALLOWLIST" "$FILE" | grep --perl-regexp "$REGEX"
    echo
    ERRORS=$((ERRORS+1))
  fi
done <<< $(find "$DIR" -name '*.rst')

if [ $ERRORS -gt 0 ]; then
  echo "ERROR: $ERRORS file(s) found with errors, see above."
  echo "You can add lines to \`$ALLOWLIST' to ignore false positives."
  exit 1
fi
