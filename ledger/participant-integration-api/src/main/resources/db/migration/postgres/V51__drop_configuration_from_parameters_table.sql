-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- TODO append-only: add migration to H2 append only as H2 support is implemented
ALTER TABLE parameters DROP COLUMN configuration;