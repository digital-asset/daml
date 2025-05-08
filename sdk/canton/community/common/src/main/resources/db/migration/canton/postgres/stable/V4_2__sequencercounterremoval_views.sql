-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create or replace view debug.sequencer_lower_bound as
  select
    single_row_lock,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(latest_topology_client_timestamp) as latest_topology_client_timestamp
  from sequencer_lower_bound;

