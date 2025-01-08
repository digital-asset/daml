#!/usr/bin/env bash
set -euo pipefail

# Run conformance tests against the Ledger API.
# https://docs.daml.com/tools/ledger-api-test-tool/index.html

jar_file=lapitt.jar

echo "### Running Ledger API conformance tests 🛠️"
java -jar "${jar_file}" "${@:1}" localhost:10011
java -jar "${jar_file}" "${@:1}" localhost:10021
