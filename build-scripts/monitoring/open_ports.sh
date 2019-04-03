#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -u

# options
PORT_RANGES=${PORT_RANGES:-"8000:8100 9000:9100 20000:20100 6865:6866"}
SHOULD_FAIL_BUILD=${SHOULD_FAIL_BUILD:-true}
SHOULD_KILL_PROCESSES=${SHOULD_KILL_PROCESSES:-false}

# internal global variables
PROCESS_DETECTED=false

function scan_ports()
{
  local port_range=$1
  local start_port=${port_range%%:*}
  local end_port=${port_range##*:}
  local sleep_before_reporting_process=10

  echo -e "\nStarting scanning port range $start_port:$end_port ... \n"

  for port in $(seq $start_port $end_port); do
    pid=$(lsof -stcp:LISTEN -t -i :$port)
    if [[ $? -eq 0 ]]; then
      echo "process $pid detected on port $port, waiting for $sleep_before_reporting_process seconds ... "
      sleep $sleep_before_reporting_process
      pid=$(lsof -stcp:LISTEN -t -i :$port)
      if [[ $? -eq 0 ]]; then
        echo "process $pid still running and will be reported ..."
        PROCESS_DETECTED=true
        ps -wwfp $pid
        if [[ $SHOULD_KILL_PROCESSES == true ]]; then
          echo -e "\nKilling process $pid\n"
          kill -9 $pid
          pid=$(lsof -stcp:LISTEN -t -i :$port)
          if [[ $? -eq 0 ]]; then
            echo -e "\nCouldnt kill the process $pid with kill -9 \n"
          else
            echo -e "\n Able to force kill process $pid with kill -9 \n"
            PROCESS_DETECTED=false
          fi
        fi
      fi
    fi
  done
}

### MAIN

echo "
Checking for open ports (tcp:LISTENING)

This is useful to invoke as a diagnostic tool before running
processes expecting fixed ports to be available.

Using the following options:

- PORT_RANGES=${PORT_RANGES}
- SHOULD_FAIL_BUILD=${SHOULD_FAIL_BUILD}
- SHOULD_KILL_PROCESSES=${SHOULD_KILL_PROCESSES}

You can override the values by using environment variables.

You can specify multiple port ranges, separated by space ' '
e.g. PORT_RANGES=\"8000:8100 8200:8300\"
"

for port_range in $PORT_RANGES; do
  scan_ports $port_range
done

if [[ $PROCESS_DETECTED == true ]]; then
  if [[ $SHOULD_FAIL_BUILD == true ]]; then
    echo -e "\n\nWill fail build as hanging processes have been detected"
    exit 1
  fi
else
  echo -e "\n\nNo processes have been detected on $PORT_RANGES port range"
fi

exit 0
