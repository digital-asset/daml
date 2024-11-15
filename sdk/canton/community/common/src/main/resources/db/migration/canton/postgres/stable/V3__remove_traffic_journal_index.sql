-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This index is sometimes erroneously used when the (member, sequencing_timestamp) index should be used.
drop index seq_traffic_control_consumed_journal_sequencing_timestamp_idx;
