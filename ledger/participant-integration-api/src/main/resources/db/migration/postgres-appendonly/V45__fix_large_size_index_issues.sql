-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX participant_command_completio_completion_offset_application_idx;
CREATE INDEX ON participant_command_completions(completion_offset, application_id);
