-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop index if exists participant_events_template_id_idx;
create index on participant_events(template_id);

drop index if exists participant_events_contract_id_idx;
create index on participant_events(contract_id);
