-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V8: Participant ID
--
-- This schema version adds a column to the parameters table
-- to store the participant ID.
---------------------------------------------------------------------------------------------------

ALTER TABLE parameters
ADD participant_id varchar;