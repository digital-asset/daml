-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table sequencer_event_recipients (
  ts bigint not null,
  recipient_id integer not null,
  node_index smallint not null,
  is_topology_event boolean not null,
  primary key (node_index, recipient_id, ts)
);

with
  sequencers as (
    select array_agg(id)
    from sequencer_members
    where substring(member from 1 for 3) = 'SEQ'
  )
insert into sequencer_event_recipients (ts, recipient_id, node_index, is_topology_event)
select
  ts,
  UNNEST(recipients) AS recipient_id,
  node_index as node_index,
  recipients && (select * from sequencers) and cardinality(recipients[1:2]) > 1 as is_topology_event
from
  sequencer_events
where
  recipients is not null;

-- create a partial index for when we specifically query only for topology relevant events
create index sequencer_event_recipients_node_recipient_topology_ts
  on sequencer_event_recipients (node_index, recipient_id, is_topology_event, ts)
  where is_topology_event is true;


create or replace view debug.sequencer_event_recipients as
select
  debug.canton_timestamp(ts) as ts,
  debug.resolve_sequencer_member(recipient_id) as recipient_id,
  node_index,
  is_topology_event
from sequencer_event_recipients;

