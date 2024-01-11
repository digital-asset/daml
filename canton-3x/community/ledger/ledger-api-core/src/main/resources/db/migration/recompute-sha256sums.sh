#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euxo pipefail

cd "$(dirname "${0}")"
find . -type f -name "*.sql" | xargs sha256sum | awk '{print "echo " $1 " > " $2}' | sed 's/\.sql/\.sha256/' | sh

