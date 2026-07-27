#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Script to upload the  test DARs to the ledger

jar_file=lapitt.jar

echo "### Extracting DARs from Ledger API conformance tests 🛠️"
java -jar "${jar_file}" -x

for i in *.dar; do
    [ -f "$i" ] || break
    daml ledger upload-dar --host localhost --port 10011 "$i"
    daml ledger upload-dar --host localhost --port 10021 "$i"
    rm "$i"
done

echo '### Done'
