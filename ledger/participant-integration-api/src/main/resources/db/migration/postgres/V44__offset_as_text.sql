-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- change offset columns to text
--

ALTER TABLE configuration_entries
    ALTER COLUMN ledger_offset TYPE varchar using encode(ledger_offset, 'hex');


ALTER TABLE package_entries
    ALTER COLUMN ledger_offset TYPE varchar using encode(ledger_offset, 'hex');

ALTER TABLE packages
    ALTER COLUMN ledger_offset TYPE varchar using encode(ledger_offset, 'hex');


ALTER TABLE parameters
    ALTER COLUMN ledger_end TYPE varchar using encode(ledger_end, 'hex'),
    ALTER COLUMN participant_pruned_up_to_inclusive TYPE varchar using encode(ledger_end, 'hex');


ALTER TABLE participant_command_completions
    ALTER COLUMN completion_offset TYPE varchar using encode(completion_offset, 'hex');

ALTER TABLE participant_events
    ALTER COLUMN event_offset TYPE varchar using encode(event_offset, 'hex'),
    ALTER COLUMN create_consumed_at TYPE varchar using encode(create_consumed_at, 'hex');


ALTER TABLE party_entries
    ALTER COLUMN ledger_offset TYPE varchar using encode(ledger_offset, 'hex');

ALTER TABLE parties
    ALTER COLUMN ledger_offset TYPE varchar using encode(ledger_offset, 'hex');

-- UPDATE parameters SET participant_pruned_up_to_inclusive='' WHERE participant_pruned_up_to_inclusive IS NULL;
