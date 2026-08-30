#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Run conformance tests against the Ledger API.
# https://docs.daml.com/tools/ledger-api-test-tool/index.html

jar_file=lapitt.jar

echo "### Running Ledger API conformance tests 🛠️"
java -jar "${jar_file}" "${@:1}" "localhost:10011;localhost:10012"
java -jar "${jar_file}" "${@:1}" "localhost:10021;localhost:10022"
