-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- add the column with the default value false
alter table mediator_synchronizer_configuration
add column is_topology_initialized bool not null default false;
