-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- no deactivation index for pruning
alter table contracts add column contract_salt binary large object;

-- Renaming the domain sequencer config to the domain manager configuration table (as we've been using it for that)
ALTER TABLE domain_sequencer_config RENAME TO domain_manager_node_settings;
ALTER TABLE domain_manager_node_settings ADD static_domain_parameters binary large object not null;
create table domain_node_settings (
    lock char(1) not null default 'X' primary key check (lock = 'X'),
    static_domain_parameters binary large object not null);
