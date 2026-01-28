-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- More indices to support pruning of ACS commitments

drop index unique_interval_participant;
drop index idx_par_outstanding_acs_commitments_by_time;
-- from_exclusive comes before to_inclusive to support the query for `noOutstandingCommitments`
create unique index idx_par_outstanding_acs_commitments on par_outstanding_acs_commitments (synchronizer_idx, from_exclusive, to_inclusive, counter_participant);

create index idx_par_computed_acs_commitments_by_time on par_computed_acs_commitments (synchronizer_idx, to_inclusive);

create index idx_par_received_acs_commitments_by_time on par_received_acs_commitments (synchronizer_idx, to_inclusive);
