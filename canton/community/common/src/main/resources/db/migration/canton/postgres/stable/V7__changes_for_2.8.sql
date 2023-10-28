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

-- changes to the event logs to accommodate more complex offsets (for topology events)
ALTER TABLE event_log ADD COLUMN local_offset_effective_time bigint NOT NULL DEFAULT 0; -- timestamp, micros from epoch
ALTER TABLE event_log ADD COLUMN local_offset_discriminator smallint NOT NULL DEFAULT 0; -- 0 for requests, 1 for topology events
ALTER TABLE event_log RENAME COLUMN local_offset TO local_offset_tie_breaker;
ALTER TABLE linearized_event_log ADD COLUMN local_offset_effective_time bigint NOT NULL DEFAULT 0; -- timestamp, micros from epoch
ALTER TABLE linearized_event_log ADD COLUMN local_offset_discriminator smallint NOT NULL DEFAULT 0; -- 0 for requests, 1 for topology events
ALTER TABLE linearized_event_log RENAME COLUMN local_offset TO local_offset_tie_breaker;

-- changes to the indexes, keys and constraints of the event logs
ALTER TABLE linearized_event_log DROP CONSTRAINT foreign_key_event_log;
DROP INDEX idx_linearized_event_log_offset;

CREATE UNIQUE INDEX idx_linearized_event_log_offset ON linearized_event_log (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker);

ALTER TABLE event_log DROP CONSTRAINT event_log_pkey;
ALTER TABLE event_log ADD PRIMARY KEY (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker);
CREATE INDEX idx_event_log_local_offset ON event_log (local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker);

ALTER TABLE linearized_event_log
  ADD CONSTRAINT foreign_key_event_log FOREIGN KEY (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker)
  REFERENCES event_log(log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) ON DELETE CASCADE;

ALTER TABLE sequencer_events ADD COLUMN error bytea;
