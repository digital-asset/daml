-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add application_id to running_triggers table defaulting to trigger_instance
alter table ${table.prefix}running_triggers add column read_as text;
update ${table.prefix}running_triggers set read_as = '';
alter table ${table.prefix}running_triggers alter column read_as set not null;
