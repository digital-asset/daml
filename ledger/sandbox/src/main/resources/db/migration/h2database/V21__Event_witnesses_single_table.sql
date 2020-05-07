-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop table participant_event_witnesses_complement;

create table participant_event_transaction_tree_witnesses
(
    event_id varchar not null,
    event_witness varchar not null,

    primary key (event_id, event_witness),
    foreign key (event_id) references participant_events(event_id)
);

drop table participant_event_flat_transaction_witnesses;

create table participant_event_flat_transaction_witnesses
(
    event_id varchar not null,
    event_witness varchar not null,

    primary key (event_id, event_witness),
    foreign key (event_id) references participant_events(event_id)
);