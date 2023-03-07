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
that are sparse in the set of all the events stored in the index db.
For transactions streams that use very large filters (with hundreds of parties and/or templates) we expect 
an increased latency for streaming the first element.

There is no change in the streaming Ledger API, i.e. no RPCs and protobuf messages have been changed. 
However, the set of the exposed metrics (related to the Index DB) was changed.

#### Impact
Each of the following metrics is a family of metrics that start with that prefix.

The following index db metrics were added:
- daml_index_db_flat_transactions_stream_translation*
- daml_index_db_flat_transactions_stream_fetch_event_consuming_ids_stakeholder*
- daml_index_db_flat_transactions_stream_fetch_event_consuming_payloads*
- daml_index_db_flat_transactions_stream_fetch_event_create_ids_stakeholder*
- daml_index_db_flat_transactions_stream_fetch_event_create_payloads*
- daml_index_db_tree_transactions_stream_translation*
- daml_index_db_tree_transactions_stream_fetch_event_consuming_ids_non_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_consuming_ids_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_consuming_payloads*
- daml_index_db_tree_transactions_stream_fetch_event_create_ids_non_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_create_ids_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_create_payloads*
- daml_index_db_tree_transactions_stream_fetch_event_non_consuming_ids_informee*
- daml_index_db_tree_transactions_stream_fetch_event_non_consuming_payloads*

The following index db metrics were removed:
- daml_index_db_get_flat_transactions*
- daml_index_db_get_transaction_trees*

### Changes to the metrics 

The following metrics were added:
- daml.identity_provider_config_store.*
- daml_index_db_flat_transactions_stream_translation*
- daml_index_db_flat_transactions_stream_fetch_event_consuming_ids_stakeholder*
- daml_index_db_flat_transactions_stream_fetch_event_consuming_payloads*
- daml_index_db_flat_transactions_stream_fetch_event_create_ids_stakeholder*
- daml_index_db_flat_transactions_stream_fetch_event_create_payloads*
- daml_index_db_tree_transactions_stream_translation*
- daml_index_db_tree_transactions_stream_fetch_event_consuming_ids_non_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_consuming_ids_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_consuming_payloads*
- daml_index_db_tree_transactions_stream_fetch_event_create_ids_non_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_create_ids_stakeholder*
- daml_index_db_tree_transactions_stream_fetch_event_create_payloads*
- daml_index_db_tree_transactions_stream_fetch_event_non_consuming_ids_informee*
- daml_index_db_tree_transactions_stream_fetch_event_non_consuming_payloads*

The following metrics were removed:
- daml_index_db_get_flat_transactions*
- daml_index_db_get_transaction_trees*

## Daml Script

#### New features:

    trigger now always batches messages (previously batching only occurred under back pressure)
    trigger configurable hard limits for (N.B. if any of these are exceeded, the trigger service will stop the trigger instance):

    ACS size has an upper bound defined (enabled and set to 20,000 active contracts by default) - Daml user code may check maxActiveContracts for warnings
    in-flight commands have an upper limit (enabled and set to 10,000 by default) - Daml user code may check maxInFlightCommands for warnings
    period of time a rule evaluation may take (disabled and set to 7.5s by default)
    period of time it takes for a rule step to be performed (disabled and set to 75ms by default)

    trigger logging context now include rule evaluation metric information (at INFO level and in a .trigger.metrics blocks) for:

    number of submissions a rule emits (with a breakdown by number of creates, exercises, etc.)
    time a rule evaluation takes
    number of steps in each rule evaluation (along with period of time each step took)
    number of get time calls made in a rule evaluation
    number of active and pending contracts at start and end of a rule evaluation
    number of in-flight commands at start and end of a rule evaluation
    size of a message batch for rule evaluation (with a breakdown by number of completion successes, completion failures, transaction creates, transaction archives and heartbeats)

    trigger TRACE logging now logs the full trigger state and the full message batch (this can be very noisy)

#### Bug fixes:

    release logback.xml now logs context and trigger logging is INFO by default
    potential trigger OOM issue in converter
    trigger context now correctly logged with Daml calls to debugRaw
    transaction sources now always DEBUG log the message they have received

### Compiler

#### New

    Daml-LF 1.15 is the default ouput of the compiler

    Interface: add support for requires construct


### IDE

    interpretation of script running for more than 60s are interrupted (no more cray java process running in background)