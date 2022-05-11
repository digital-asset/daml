--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

UPDATE parameters SET ledger_end = '' WHERE ledger_end IS NULL;
UPDATE parameters SET ledger_end_sequential_id = 0 WHERE ledger_end_sequential_id IS NULL;
UPDATE parameters SET ledger_end_string_interning_id = 0 WHERE ledger_end_string_interning_id IS NULL;

ALTER TABLE parameters ALTER COLUMN ledger_end SET NOT NULL;
ALTER TABLE parameters ALTER COLUMN ledger_end_sequential_id SET NOT NULL;
ALTER TABLE parameters ALTER COLUMN ledger_end_string_interning_id SET NOT NULL;
