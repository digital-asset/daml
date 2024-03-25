#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Once we have GA the following holds:
# The Flyway SQL migration scripts which have corresponding .sha256 files are frozen (should not be functionally changed, else existing deployments will be broken).
# As a rule of thumb, we never regenerate SHA files as part of ordinary development except when we fix a typo in a comment in an existing frozen migration script.
set -euxo pipefail
DIR=$1
cd "$DIR"
find . -type f -name "*.sql" | xargs sha256sum | awk '{print "echo " $1 " > " $2}' | sed 's/\.sql/\.sha256/' | sh

