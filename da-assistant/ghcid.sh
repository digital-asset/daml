#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

[ -n "$1" ] && ARGS="--test=$1"
exec ghcid --command="$(dirname $0)/ghci.sh" "$ARGS"
