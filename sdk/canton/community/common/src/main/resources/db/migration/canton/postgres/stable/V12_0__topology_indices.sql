-- Add indices that speed up certain topology queries

-- for:
-- - DbTopologyStore.findProposalsByTxHash
-- - DbTopologyStore.findLatestTransactionsAndProposalsByTxHash
create index idx_common_topology_transactions_by_tx_hash
  on common_topology_transactions (store_id, tx_hash, is_proposal, valid_from, valid_until, rejection_reason);

-- for:
-- - DbTopologyStore.findEffectiveStateChanges
create index idx_common_topology_transactions_effective_changes
  on common_topology_transactions (store_id, is_proposal, valid_from, valid_until, rejection_reason)
  where is_proposal = false;


-- for:
-- - DbTopologyStore.update, updating the valid_until column for past transactions
create index idx_common_topology_transactions_for_valid_until_update
  on common_topology_transactions (store_id, mapping_key_hash, serial_counter, valid_from)
  where valid_until is null;
