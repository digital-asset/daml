-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE test ADD COLUMN foo2 character varying;
ALTER TABLE test DROP COLUMN foo1;
