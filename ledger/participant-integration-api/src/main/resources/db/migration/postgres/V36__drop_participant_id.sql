-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V36: Drop the participant ID columns from the configuration and party entries.
--
---------------------------------------------------------------------------------------------------

DROP INDEX idx_configuration_submission;
ALTER TABLE configuration_entries DROP COLUMN participant_id;
CREATE UNIQUE INDEX idx_configuration_submission ON configuration_entries (submission_id);

ALTER TABLE party_entries DROP COLUMN participant_id;
