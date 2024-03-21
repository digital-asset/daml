-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add access token to running trigger table
alter table ${table.prefix}running_triggers add (access_token nvarchar2(2000));
