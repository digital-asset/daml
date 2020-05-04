#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


echo "nix" > "$1/private/etc/synthetic.conf"

# taken from https://www.postgresql.org/docs/12/kernel-resources.html
cat > "$1/private/etc/sysctl.conf" <<END
kern.sysv.shmmax=4194304
kern.sysv.shmmin=1
kern.sysv.shmmni=32
kern.sysv.shmseg=8
kern.sysv.shmall=1024
END
