-- An additional index to speed up data fetching for the acs commitment processor
create index idx_active_contracts_changes_between on active_contracts(domain_id, ts, request_counter, contract_id, change desc) include (operation);
