-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Change indexed bytea columns to varchar.

ALTER TABLE configuration_entries
    ALTER COLUMN ledger_offset TYPE VARCHAR USING encode(ledger_offset, 'hex');


ALTER TABLE package_entries
    ALTER COLUMN ledger_offset TYPE VARCHAR USING encode(ledger_offset, 'hex');

ALTER TABLE packages
    ALTER COLUMN ledger_offset TYPE VARCHAR USING encode(ledger_offset, 'hex');


ALTER TABLE parameters
    ALTER COLUMN ledger_end TYPE VARCHAR USING encode(ledger_end, 'hex'),
    ALTER COLUMN participant_pruned_up_to_inclusive TYPE VARCHAR USING encode(participant_pruned_up_to_inclusive, 'hex');


ALTER TABLE participant_command_completions
    ALTER COLUMN completion_offset TYPE VARCHAR USING encode(completion_offset, 'hex');

ALTER TABLE participant_contracts
    ALTER COLUMN create_key_hash TYPE VARCHAR USING encode(create_key_hash, 'hex');;

ALTER TABLE participant_events
    ALTER COLUMN event_offset TYPE VARCHAR USING encode(event_offset, 'hex'),
    ALTER COLUMN create_consumed_at TYPE VARCHAR USING encode(create_consumed_at, 'hex');


ALTER TABLE party_entries
    ALTER COLUMN ledger_offset TYPE VARCHAR USING encode(ledger_offset, 'hex');

ALTER TABLE parties
    ALTER COLUMN ledger_offset TYPE VARCHAR USING encode(ledger_offset, 'hex');
