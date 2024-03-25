#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -eux

NEW_KEY=$(dd if=/dev/urandom bs=1 count=16 | xxd -p -u)
sed -i '' -e "s/5731486D346B5949685A7A3836736965/${NEW_KEY}/g" deployment/scripts/shell/package.sh
