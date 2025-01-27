#!/usr/bin/env bash
set -euo pipefail

# Query Prometheus for the daml_health_status metric and validate that it has the value 1,
# representing a healthy service

retry=0
result=''

until [[ "${retry}" -ge 10 ]]; do
  # Query Prometheus for daml_health_status >= 1
  result=$(curl -sS --retry 10 --retry-delay 10 --retry-connrefused 'http://localhost:9090/api/v1/query?query=daml_health_status+%3E%3D+1')
  # http-json, canton synchronizer and participant
  # trigger-service does not expose health data
  if [[ $(echo "${result}" | jq -r '.data.result | length') -eq 3 ]]; then
    echo "Services are registered healthy in prometheus."
    echo "${result}"
    exit 0
  else
    echo "No healthy service yet, retrying."
    echo "${result}"
  fi
  retry=$((retry++))
  sleep 10
done

echo "No healthy service was found in the metrics."
exit 1
