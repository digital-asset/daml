--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0


-- Note: can't make the ledger_end column NOT NULL, as we need to be able to insert the empty string

UPDATE parameters SET ledger_end_sequential_id = 0 WHERE ledger_end_sequential_id IS NULL;
UPDATE parameters SET ledger_end_string_interning_id = 0 WHERE ledger_end_string_interning_id IS NULL;

ALTER TABLE parameters MODIFY ( ledger_end_sequential_id NOT NULL);
ALTER TABLE parameters MODIFY ( ledger_end_string_interning_id NOT NULL);

