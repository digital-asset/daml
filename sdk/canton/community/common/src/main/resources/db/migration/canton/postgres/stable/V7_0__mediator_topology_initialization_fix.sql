-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- add the column with the default value true, so that
-- any existing connection is considered initialized
alter table mediator_synchronizer_configuration
  add column is_topology_initialized bool not null default true;

-- now set the default back to false
alter table mediator_synchronizer_configuration
  alter column is_topology_initialized set default false;


create or replace view debug.mediator_synchronizer_configuration as
select
  lock,
  synchronizer_id,
  static_synchronizer_parameters,
  sequencer_connection,
  is_topology_initialized
from mediator_synchronizer_configuration;
