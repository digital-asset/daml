-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE transfer_causality_updates;

-- update the sequencer_state_manager_events to allow storing tombstones making content optional
ALTER TABLE sequencer_state_manager_events ALTER COLUMN content DROP NOT NULL;
ALTER TABLE sequencer_state_manager_events ADD COLUMN tombstone_message varchar(300) DEFAULT NULL;
ALTER TABLE sequencer_state_manager_events ADD CONSTRAINT chk_content_xor_tombstone
  CHECK ((content IS NOT NULL AND tombstone_message IS NULL) OR (content IS NULL AND tombstone_message IS NOT NULL));

-- track the age of the initial topology's timestamp or NULL if not available
-- when available, the timestamp limits how long the sequencer creates tombstones.
ALTER TABLE sequencer_state_manager_lower_bound ADD COLUMN ts_initial_topology BIGINT NULL;

ALTER TABLE fresh_submitted_transaction_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE active_contract_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE commitment_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE contract_key_pruning ADD COLUMN succeeded bigint null;
ALTER TABLE sequenced_event_store_pruning ADD COLUMN succeeded bigint null;



