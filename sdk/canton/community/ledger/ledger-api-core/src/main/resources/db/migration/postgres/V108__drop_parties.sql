-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE parties;
CREATE INDEX idx_party_entries_party_and_ledger_offset ON party_entries(party, ledger_offset);
