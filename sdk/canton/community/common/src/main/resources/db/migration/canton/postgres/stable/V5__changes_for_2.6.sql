-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE pruning_schedules(
  -- node_type is one of "PAR", "MED", or "SEQ"
  -- since mediator and sequencer sometimes share the same db
  node_type varchar(3) collate "C" not null primary key,
  cron varchar(300) collate "C" not null,
  max_duration bigint not null, -- positive number of seconds
  retention bigint not null -- positive number of seconds
);

ALTER TABLE participant_settings ADD max_burst_factor double precision not null default 0.5;
