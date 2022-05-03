-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX idx_package_entries;
CREATE INDEX idx_package_entries ON package_entries (submission_id);
DROP INDEX idx_configuration_submission;
CREATE INDEX idx_configuration_submission ON configuration_entries (submission_id);
DROP INDEX idx_party_entries;
CREATE INDEX idx_party_entries ON party_entries (submission_id);
