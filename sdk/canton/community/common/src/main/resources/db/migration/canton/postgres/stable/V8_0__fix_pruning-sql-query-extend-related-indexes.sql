-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX lapi_command_completions_synchronizer_record_time_idx;
CREATE INDEX lapi_command_completions_synchronizer_record_time_offset_idx ON lapi_command_completions USING btree (synchronizer_id, record_time, completion_offset);

DROP INDEX lapi_transaction_meta_synchronizer_record_time_idx;
CREATE INDEX lapi_transaction_meta_synchronizer_record_time_offset_idx ON lapi_transaction_meta USING btree (synchronizer_id, record_time, event_offset);
