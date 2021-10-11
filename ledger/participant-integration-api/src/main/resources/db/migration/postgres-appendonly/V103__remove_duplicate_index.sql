-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V103: Remove duplicate index
--
-- The following index was added both in V45 and V100.2, drop one of them.
---------------------------------------------------------------------------------------------------


DROP INDEX participant_command_completio_completion_offset_application_idx;
