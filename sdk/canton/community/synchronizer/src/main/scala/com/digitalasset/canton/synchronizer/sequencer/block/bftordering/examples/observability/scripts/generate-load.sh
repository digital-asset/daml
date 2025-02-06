#!/usr/bin/env bash
set -euo pipefail

# Script to generate load by running conformance tests multiple times (defaults to 10)

# Full path to this script
current_dir=$(cd "$(dirname "${0}")" && pwd)

loops="${1:-10}"
case "${loops}" in
  ''|*[!0-9]*) echo 'Error: first argument must be an integer'; exit 1;;
  *) echo "### Running conformance tests ${loops} times";;
esac

for i in $(seq 1 "${loops}")
do
  echo '################################################################################'
  echo "### Iteration ${i} (out of ${loops})"
  "${current_dir}/test-ledger-api.sh" "${@:2}"
done

echo '### Done'
