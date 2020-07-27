#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
usage() {
  cat >&2 <<'EOF'
usage: mktgz OUTPUT ARGS...

Creates a gzip compressed tarball in OUTPUT passing ARGS to tar. The created
tarball is reproducible, i.e. it does not contain any timestamps or similar
non-deterministic inputs. See https://reproducible-builds.org/docs/archives/
EOF
}
trap usage ERR

tar c "${@:2}" \
  --owner="0" \
  --group="0" \
  --numeric-owner \
  --mtime="2000-01-01 00:00Z" \
  --no-acls \
  --no-xattrs \
  --no-selinux \
  --sort="name" \
  --format=ustar \
  | gzip -n > "$1"
