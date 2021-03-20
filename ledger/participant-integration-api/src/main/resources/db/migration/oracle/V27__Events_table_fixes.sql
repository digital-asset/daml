-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V27: Events table fixes
--
-- * Remove `is_root` from the index events table (https://github.com/digital-asset/daml/issues/5618)
-- * Merge template identifier columns in index events table (https://github.com/digital-asset/daml/issues/5619)
---------------------------------------------------------------------------------------------------

-- already covered in V20__Events_new_schema.sql
