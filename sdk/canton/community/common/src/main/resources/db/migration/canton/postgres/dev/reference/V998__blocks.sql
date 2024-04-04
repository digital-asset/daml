-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates
--
-- Proprietary code. All rights reserved.

create table blocks (
  id bigint primary key,
  request bytea not null,
  uuid varchar(36) collate "C" unique not null
);
