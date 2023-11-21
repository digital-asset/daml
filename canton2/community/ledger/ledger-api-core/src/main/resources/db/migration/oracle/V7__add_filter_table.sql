--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE participant_events_create_filter (
    event_sequential_id NUMBER NOT NULL,
    template_id NUMBER NOT NULL,
    party_id NUMBER NOT NULL
);
CREATE INDEX idx_participant_events_create_filter_party_template_seq_id_idx ON participant_events_create_filter(party_id, template_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_filter_party_seq_id_idx ON participant_events_create_filter(party_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_seq_id_idx ON participant_events_create_filter(event_sequential_id);
