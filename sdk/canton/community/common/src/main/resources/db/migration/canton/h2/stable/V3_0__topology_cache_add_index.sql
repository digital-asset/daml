-- for:
-- - DbTopologyStore.maxTimestamp and DbTopologyStore.latestTopologyChangeTimestamp
create index idx_common_topology_transactions_max_timestamp on common_topology_transactions (store_id, sequenced);
