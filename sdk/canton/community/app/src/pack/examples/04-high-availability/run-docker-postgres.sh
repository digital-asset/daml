#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

docker run --name postgres --rm \
  -e POSTGRES_PASSWORD=supersafe \
  -p 5432:5432 \
  -v "$PWD/postgres-docker-initdb.sh:/docker-entrypoint-initdb.d/postgres-docker-initdb.sh" \
  postgres:13

