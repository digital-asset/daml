--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE participant_events_create_filter (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL
);

SELECT 'Add Filter Table: Migrating to participant_events_create_filter table...';

INSERT INTO participant_events_create_filter(event_sequential_id, template_id, party_id)
SELECT event_sequential_id, template_id, unnest(flat_event_witnesses)
FROM participant_events_create
ORDER BY event_sequential_id;

SELECT 'Add Filter Table: Creating indexes...';

CREATE INDEX idx_participant_events_create_filter_party_template_seq_id_idx ON participant_events_create_filter(party_id, template_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_filter_party_seq_id_idx ON participant_events_create_filter(party_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_seq_id_idx ON participant_events_create_filter(event_sequential_id);

SELECT 'Add Filter Table: Done.';
