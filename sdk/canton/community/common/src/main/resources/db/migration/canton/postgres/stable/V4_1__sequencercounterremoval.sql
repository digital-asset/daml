-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop index idx_sequencer_counter_checkpoints_by_member_ts;
drop index idx_sequencer_counter_checkpoints_by_ts;
drop table sequencer_counter_checkpoints
    -- cascade is necessary to simultaneously drop the debug view if it's defined
    cascade;

alter table sequencer_lower_bound
    add column latest_topology_client_timestamp bigint;
