..
   Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

Daml Ledger Configuration Specification
=======================================

**version 1, 25th November 2019**

This specification, along with ``ledger_configuration.proto``
defines the data representation and the semantics of ledger
configuration, including the time model.

We follow the same rules as ``transaction.proto`` with regards
to assignment of version numbers and maintaining backwards compatibility.
Please read ``daml-lf/spec/transaction.rst`` to understand the rules
we follow for evolving the ledger configuration.

The canonical specification compliant implementation for encoding and
decoding ledger configurations is part of the ledger-configuration
package in ``ledger/ledger-configuration``.

Version history
^^^^^^^^^^^^^^^

Please refer to ``ledger_configuration.proto`` for version history.

message LedgerConfiguration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

*since version 1*

An instance of the ledger configuration.

As of version 1, these fields are included:
* ``int64`` version
* ``int64`` generation
* ``LedgerTimeModel`` time_model
* ``string`` authorized_participant_id

``version`` is required, and should be set to the latest version as
specified by this document. Consumers should reject configurations
with an unknown version.

``generation`` is required and must be set to a number one larger than
the current configuration in order for the configuration change to be
accepted. This safe-guards against configuration changes based on
stale data.

``time_model`` is required.

*since version 2*

``max_deduplication_duration`` is required and must be set to a positive duration.
This defines the maximum value for the corresponding ``deduplication_duration``
parameter of command submissions, i.e., the maximum time during which a command
can be deduplicated.


message LedgerTimeModel
^^^^^^^^^^^^^^^^^^^^^^^

*since version 1*

Defines the ledger time model, which governs the rules for acceptable
ledger time and maximum record time parameters that are part
of transaction submission.

As of version 1, these fields are included:

* ``Duration`` min_transaction_latency
* ``Duration`` max_clock_skew
* ``Duration`` max_ttl

``min_transaction_latency`` is required. It defines the minimum expected
latency for a transaction to be committed.

``max_clock_skew`` is required. It defines the maximum allowed clock skew
between the ledger and clients.

``max_ttl`` is required. It defines the maximum allowed time to live for a
transaction.

These three parameters are used in the following way.
Given the record time ``RT``, a transaction submission with a ledger effective time ``LET``
 and maximum record time ``MRT`` is accepted if:

* ``LET - MRT >= min_transaction_latency + max_clock_skew``
* ``LET - MRT <= max_ttl``.
* ``LET - max_clock_skew <= RT <= MRT``.
