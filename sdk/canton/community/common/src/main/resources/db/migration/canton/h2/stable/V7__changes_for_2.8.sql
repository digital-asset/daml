-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE transfer_causality_updates;

-- track the age of the initial topology's timestamp or NULL if not available
-- when available, the timestamp limits how long the sequencer creates tombstones.
ALTER TABLE sequencer_state_manager_lower_bound ADD COLUMN ts_initial_topology BIGINT NULL;

ALTER TABLE fresh_submitted_transaction_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE active_contract_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE commitment_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE contract_key_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE sequenced_event_store_pruning ADD COLUMN succeeded bigint null;

ALTER TABLE sequencer_events ADD COLUMN error binary large object;

-- participant_pruning_schedules with pruning flag specific to participant pruning
CREATE TABLE participant_pruning_schedules (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  cron varchar(300) not null,
  max_duration bigint not null, -- positive number of seconds
  retention bigint not null, -- positive number of seconds
  prune_internally_only boolean NOT NULL DEFAULT false -- whether to prune only canton-internal stores not visible to ledger api
);
-- move participant pruning schedules identified by ParticipantId.Code PAR to new table
INSERT INTO participant_pruning_schedules (cron, max_duration, retention)
  SELECT cron, max_duration, retention FROM pruning_schedules WHERE node_type = 'PAR';
DELETE FROM pruning_schedules WHERE node_type = 'PAR';
