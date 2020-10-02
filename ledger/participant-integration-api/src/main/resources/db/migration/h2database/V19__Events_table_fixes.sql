-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V19: Events table fixes
--
-- * Remove `is_root` from the index events table (https://github.com/digital-asset/daml/issues/5618)
-- * Merge template identifier columns in index events table (https://github.com/digital-asset/daml/issues/5619)
---------------------------------------------------------------------------------------------------

set referential_integrity false;

ALTER TABLE participant_events DROP COLUMN is_root;

ALTER TABLE participant_events DROP COLUMN template_package_id;
ALTER TABLE participant_events DROP COLUMN template_name;
ALTER TABLE participant_events ADD COLUMN template_id VARCHAR;
CREATE INDEX ON participant_events(template_id);

set referential_integrity true;
