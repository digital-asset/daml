-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE INDEX active_contracts_pruning_idx on active_contracts (domain_id, ts) WHERE change = 'deactivation';

ALTER TABLE contracts ADD COLUMN contract_salt bytea;

-- Renaming the domain sequencer config to the domain manager configuration table (as we've been using it for that)
ALTER TABLE domain_sequencer_config RENAME TO domain_manager_node_settings;
-- TODO(#9014) change to non null with 3.0
ALTER TABLE domain_manager_node_settings ADD static_domain_parameters bytea null;

create table domain_node_settings (
    lock char(1) not null default 'X' primary key check (lock = 'X'),
    static_domain_parameters bytea not null);
