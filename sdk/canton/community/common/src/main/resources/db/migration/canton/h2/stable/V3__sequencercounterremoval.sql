-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop index idx_sequencer_counter_checkpoints_by_member_ts on sequencer_counter_checkpoints;
drop table sequencer_counter_checkpoints;

alter table sequencer_lower_bound
  add column latest_topology_client_timestamp bigint;
