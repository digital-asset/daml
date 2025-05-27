-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table sequencer_event_recipients (
  ts bigint not null,
  recipient_id integer not null,
  node_index smallint not null,
  is_topology_event boolean not null,
  primary key (node_index, recipient_id, ts)
);

create index sequencer_event_recipients_node_recipient_topology_ts
  on sequencer_event_recipients (node_index, recipient_id, is_topology_event, ts)
