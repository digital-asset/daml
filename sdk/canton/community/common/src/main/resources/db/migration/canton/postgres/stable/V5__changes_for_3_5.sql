-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP VIEW debug.common_party_metadata;
DROP TABLE common_party_metadata;

alter table seq_block_height add column latest_pending_topology_ts bigint;

create or replace view debug.seq_block_height as
select
    height,
    debug.canton_timestamp(latest_event_ts) as latest_event_ts,
    debug.canton_timestamp(latest_sequencer_event_ts) as latest_sequencer_event_ts,
    debug.canton_timestamp(latest_pending_topology_ts) as latest_pending_topology_ts
from seq_block_height;
