-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE participant_events ADD COLUMN create_argument_compression SMALLINT;
ALTER TABLE participant_events ADD COLUMN create_key_value_compression SMALLINT;
ALTER TABLE participant_events ADD COLUMN exercise_argument_compression SMALLINT;
ALTER TABLE participant_events ADD COLUMN exercise_result_compression SMALLINT;

ALTER TABLE participant_contracts ADD COLUMN create_argument_compression SMALLINT;
