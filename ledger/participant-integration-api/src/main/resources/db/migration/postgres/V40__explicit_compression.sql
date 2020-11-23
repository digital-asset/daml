-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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
