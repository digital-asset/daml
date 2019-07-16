#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ENV_FILE=$1
ARGS_FILE=$2
echo "{ENV}" > "$ENV_FILE"
echo "{ARGS}" > "$ARGS_FILE"
