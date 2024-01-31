#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

echo "nix" > "$1/private/etc/synthetic.conf"

# Increase shared memory allocation for PostGresQL on MacOS
cat > "$1/private/etc/sysctl.conf" <<END
kern.sysv.shmmax=16777216
kern.sysv.shmmin=1
kern.sysv.shmmni=128
kern.sysv.shmseg=32
kern.sysv.shmall=4096
END
