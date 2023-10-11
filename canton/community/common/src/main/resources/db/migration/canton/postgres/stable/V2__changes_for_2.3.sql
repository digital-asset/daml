-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- TODO(#9014) migrate to non null
ALTER TABLE topology_transactions ADD sequenced bigint null;

-- A stored HMAC secret is not used anymore
DROP TABLE crypto_hmac_secret;

-- The column sequencer_counter_checkpoints stores the latest timestamp before or at the sequencer counter checkpoint
-- at which the original batch of a deliver event sent to the member also contained an enveloped addressed
-- to the member that updates the SequencerReader's topology client (the sequencer in case of an external sequencer
-- and the domain topology manager for embedding sequencers)
-- NULL if the sequencer counter checkpoint was generated before this column was added.
alter table sequencer_counter_checkpoints
    add column latest_topology_client_ts bigint;

-- Add state to participant domain connection configurations
ALTER TABLE participant_domain_connection_configs ADD status CHAR(1) DEFAULT 'A' NOT NULL;
ALTER TABLE participant_domains ADD status CHAR(1) DEFAULT 'A' NOT NULL;