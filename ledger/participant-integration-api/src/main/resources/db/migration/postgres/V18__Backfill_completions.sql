-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

INSERT INTO participant_command_completions (completion_offset, record_time)
SELECT ledger_offset + 1, recorded_at
FROM ledger_entries
WHERE typ = 'checkpoint';

INSERT INTO participant_command_completions (completion_offset, record_time, application_id, submitting_party, command_id, transaction_id)
SELECT ledger_offset + 1, recorded_at, application_id, submitter, command_id, transaction_id
FROM ledger_entries
WHERE typ = 'transaction';

INSERT INTO participant_command_completions (completion_offset, record_time, application_id, submitting_party, command_id, status_code, status_message)
SELECT
    ledger_offset + 1,
    recorded_at,
    application_id,
    submitter,
    command_id,
    CASE
        WHEN rejection_type in ('Inconsistent', 'Disputed', 'PartyNotKnownOnLedger') THEN 3 -- INVALID_ARGUMENT
        WHEN rejection_type in ('OutOfQuota', 'TimedOut') THEN 10 -- ABORTED
        WHEN rejection_type = 'SubmitterCannotActViaParticipant' THEN 7 -- PERMISSION_DENIED
    END,
    rejection_description
FROM ledger_entries
WHERE typ = 'rejection';
