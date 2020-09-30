#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

echo Checks closing quotes '`' for all rst file.

errors=0

daml_ascii_art='^   *[_\\/][ _\\/]'

while read file
do
  numberOfQuotes=$(egrep -v "$daml_ascii_art" "$file" | grep -o '`' | wc -l)
  if (( $numberOfQuotes % 2 ))
  then
    ((errors++))
    echo "$file" : $numberOfQuotes
    echo Suspicious lines:
    awk -F '`' 'NF % 2 == 0 && NF > 0 ' < "$file"
    echo
  fi
done <<< $(find -name '*rst')

echo $errors errors found.
echo "Note: errors are not detected if they are in the same line (of even number)."
if [ $errors -gt 0 ]
then
  exit 1
fi
