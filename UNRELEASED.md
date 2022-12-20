# Release of Daml DAML_VERSION

## What’s New

Use one of the following topics to capture your release notes. Duplicate this file if necessary.
Add [links to the documentation too](https://docs.daml.com/DAML_VERSION/about.html).

You text does not need to be perfect. Leave good information here such that we can build release notes from your hints.
Of course, perfect write-ups are very welcome.

You need to update this file whenever you commit something of significance. Reviewers need to check your PR 
and ensure that the release notes and documentation has been included as part of the PR.


### Efficient transaction queries: flat and tree transactions

We changed the internal implementation used when serving flat and tree transaction streams from the index database.
We expect a general improvement in throughput for these streams, especially for streams that fetch subsets of events
that are sparse relative to the set of all the events stored in the index db.
For transactions streams that use very large filters (with hundreds of parties and/or templates) we expect 
an increased latency of streaming the first element.

There is no change in the streaming Ledger API. 
However, the set of the exposed index db related metrics was changed.

#### Impact

The following index db metrics were added:
- daml_index_db_flat_transactions_stream
- daml_index_db_flat_transactions_stream_fetch_event_consuming_ids_stakeholder
- daml_index_db_flat_transactions_stream_fetch_event_consuming_payloads
- daml_index_db_flat_transactions_stream_fetch_event_create_ids_stakeholder
- daml_index_db_flat_transactions_stream_fetch_event_create_payloads
- daml_index_db_tree_transactions_stream
- daml_index_db_tree_transactions_stream_fetch_event_consuming_ids_non_stakeholder
- daml_index_db_tree_transactions_stream_fetch_event_consuming_ids_stakeholder
- daml_index_db_tree_transactions_stream_fetch_event_consuming_payloads
- daml_index_db_tree_transactions_stream_fetch_event_create_ids_non_stakeholder
- daml_index_db_tree_transactions_stream_fetch_event_create_ids_stakeholder
- daml_index_db_tree_transactions_stream_fetch_event_create_payloads
- daml_index_db_tree_transactions_stream_fetch_event_non_consuming_ids_informee
- daml_index_db_tree_transactions_stream_fetch_event_non_consuming_payloads

The following index db metrics were removed:
- daml_index_db_get_flat_transactions
- daml_index_db_get_transaction_trees

