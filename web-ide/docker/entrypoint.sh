#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eux

if [ "$1" = 'web-ide' ]; then
  shift
  echo "running code server with $@"
  code-server "$@"
else
  exec "$@"
fi
