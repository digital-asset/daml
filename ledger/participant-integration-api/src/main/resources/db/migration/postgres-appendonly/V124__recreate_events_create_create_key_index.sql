--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

DROP INDEX participant_events_create_create_key_hash_idx;

-- recreate the index ordering by event_sequential_id first
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create USING btree (event_sequential_id, create_key_hash);