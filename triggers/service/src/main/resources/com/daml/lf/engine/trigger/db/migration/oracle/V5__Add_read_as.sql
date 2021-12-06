-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add application_id to running_triggers table defaulting to trigger_instance
alter table ${table.prefix}running_triggers add (read_as nvarchar2(2000));
