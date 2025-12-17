-- An extra index to speed up DbTopologyStore.maxTimestamp

create index idx_common_topology_transactions_max_timestamp
on common_topology_transactions
using btree (store_id, sequenced);
