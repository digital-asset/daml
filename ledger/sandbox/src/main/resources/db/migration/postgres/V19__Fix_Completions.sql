-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

UPDATE participant_command_completions
SET completion_offset = completion_offset - 1;
