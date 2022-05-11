-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V107: Parameter table cleanup
--
-- This migration makes sure the two following invariants hold:
-- - The ledger_id and participant_id columns are always defined at the same time. I.e., either
--   the table is empty, or both are defined.
-- - The ledger_end and ledger_end_sequential_id are always defined at the same time. I.e., either
--   both are NULL, or both are defined.
-- Additionally, it removes unused columns.
---------------------------------------------------------------------------------------------------


-- It is in theory possible that the participant_id column contains a NULL value.
-- This should only happen if the first-time database initialization failed half-way between writing the
-- ledger_id and participant_id. In this case, we would ask the operator to re-create or manually edit the database.
ALTER TABLE parameters ALTER COLUMN participant_id SET NOT NULL;

-- This should only apply when a database was migrated to the append-only schema, where the database contained some
-- state updates (e.g., party allocations), but no events (i.e., no transactions).
UPDATE parameters SET ledger_end_sequential_id = 0 WHERE ledger_end_sequential_id IS NULL AND ledger_end IS NOT NULL;

-- This column is not needed anymore.
ALTER TABLE parameters DROP COLUMN external_ledger_end;