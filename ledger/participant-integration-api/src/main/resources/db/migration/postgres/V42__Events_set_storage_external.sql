-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE participant_events
    ALTER COLUMN create_argument SET STORAGE EXTERNAL,
    ALTER COLUMN create_key_value SET STORAGE EXTERNAL,
    ALTER COLUMN exercise_argument SET STORAGE EXTERNAL,
    ALTER COLUMN exercise_result SET STORAGE EXTERNAL;
