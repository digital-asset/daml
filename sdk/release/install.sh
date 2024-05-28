#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
daml_version_file="$DIR/daml_version.txt"
if [ -f "$daml_version_file" ]; then
  echo "Installing with internal version file $daml_version_file: $(cat "$daml_version_file")" 1>&2
  flag=--install-with-custom-version=$(cat "$daml_version_file")
fi
"$DIR/daml/daml" install "$DIR" ${flag:-} $@
