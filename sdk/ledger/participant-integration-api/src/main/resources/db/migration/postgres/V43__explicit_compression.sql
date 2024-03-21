-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- SET STORAGE EXTERNAL is for external (from the standpoint of TOAST), uncompressed data.
--
-- Note that SET STORAGE doesn't itself change anything in the table,
-- it just sets the strategy to be pursued during future table updates.
--
-- `SET STORAGE EXTERNAL` https://www.postgresql.org/docs/13/sql-altertable.html
-- TOAST https://www.postgresql.org/docs/13/storage-toast.html

ALTER TABLE participant_events
    ADD COLUMN   create_argument_compression SMALLINT,
    ALTER COLUMN create_argument SET STORAGE EXTERNAL,
    ADD COLUMN   create_key_value_compression SMALLINT,
    ALTER COLUMN create_key_value SET STORAGE EXTERNAL,
    ADD COLUMN   exercise_argument_compression SMALLINT,
    ALTER COLUMN exercise_argument SET STORAGE EXTERNAL,
    ADD COLUMN   exercise_result_compression SMALLINT,
    ALTER COLUMN exercise_result SET STORAGE EXTERNAL;

ALTER TABLE participant_contracts
    ADD COLUMN   create_argument_compression SMALLINT,
    ALTER COLUMN create_argument SET STORAGE EXTERNAL;
