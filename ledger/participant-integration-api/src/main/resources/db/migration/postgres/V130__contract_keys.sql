--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- key lookups on create table not needed anymore anymore
DROP INDEX participant_events_create_create_key_hash_idx;

CREATE TABLE contract_keys (
    contract_key_hash BIGINT NOT NULL,
    create_event_sequential_id BIGINT NOT NULL,
    contract_id TEXT NOT NULL
);

-- for key mapping lookup
CREATE INDEX contract_keys_lookup ON contract_keys USING BTREE (contract_key_hash, create_event_sequential_id, contract_id);
-- for pruning
CREATE INDEX contract_keys_event_lookup ON contract_keys USING BTREE (contract_id);