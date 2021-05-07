-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE ${table.prefix}state ALTER key_hash SET NOT NULL;
ALTER TABLE ${table.prefix}state DROP CONSTRAINT ${table.prefix}state_pkey;
ALTER TABLE ${table.prefix}state ADD PRIMARY KEY (key_hash);
