-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop index if exists participant_events_template_id_idx;
create index participant_events_template_id_idx on participant_events using hash(template_id);

drop index if exists participant_events_transaction_id_idx;
create index participant_events_transaction_id_idx on participant_events using hash(transaction_id);

drop index if exists participant_events_contract_id_idx;
create index participant_events_contract_id_idx on participant_events using hash(contract_id);
