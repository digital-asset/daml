-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Change indexed byte array columns to varchar.

ALTER TABLE configuration_entries
    ALTER COLUMN ledger_offset SET DATA TYPE VARCHAR;


ALTER TABLE package_entries
    ALTER COLUMN ledger_offset SET DATA TYPE VARCHAR;

ALTER TABLE packages
    ALTER COLUMN ledger_offset SET DATA TYPE VARCHAR;


ALTER TABLE parameters
    ALTER COLUMN ledger_end SET DATA TYPE VARCHAR;
ALTER TABLE parameters
    ALTER COLUMN participant_pruned_up_to_inclusive SET DATA TYPE VARCHAR;


ALTER TABLE participant_command_completions
    ALTER COLUMN completion_offset SET DATA TYPE VARCHAR;

ALTER TABLE participant_contracts
    ALTER COLUMN create_key_hash SET DATA TYPE VARCHAR;

ALTER TABLE participant_events
    ALTER COLUMN event_offset SET DATA TYPE VARCHAR;
ALTER TABLE participant_events
    ALTER COLUMN create_consumed_at SET DATA TYPE VARCHAR;


ALTER TABLE parties
    ALTER COLUMN ledger_offset SET DATA TYPE VARCHAR;

ALTER TABLE party_entries
    ALTER COLUMN ledger_offset SET DATA TYPE VARCHAR;

