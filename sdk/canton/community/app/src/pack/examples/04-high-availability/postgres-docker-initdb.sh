#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER canton WITH PASSWORD 'supersafe';

    CREATE DATABASE sequencer;
    CREATE DATABASE mediator;
    CREATE DATABASE participant;

    GRANT ALL PRIVILEGES ON DATABASE sequencer TO canton;
    GRANT ALL PRIVILEGES ON DATABASE mediator TO canton;
    GRANT ALL PRIVILEGES ON DATABASE participant TO canton;
EOSQL
