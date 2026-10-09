-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table blocks (
  id bigint primary key,
  request bytea not null,
  uuid varchar collate "C" unique not null
);
