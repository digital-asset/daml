.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Ledger Configuration Specification
=======================================

**version 1, 22nd June 2019**

This specification, along with ``ledger_configuration.proto``
defines the data representation and the semantics of ledger
configuration, including the time model.

We follow the same rules as ``transaction.proto`` with regards
to assignment of version numbers and maintaining backwards compatibility.
Please read ``daml-lf/spec/transaction.rst`` to understand the rules
we follow for evolving the ledger configuration.

The canonical specification compliant implementation for encoding and
decoding ledger configurations is part of the participant-state package
in ``ledger/participant-state``.

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
* ``bool`` open_world

``version`` is required, and should be set to the latest version as
specified by this document. Consumers should reject configurations
with an unknown version.

``generation`` is required and must be set to a number one larger than
the current configuration in order for the configuration change to be
accepted. This safe-guards against configuration changes based on
stale data.

``time_model`` is required.

``authorized_participant_id`` is optional. If non-empty, then configuration
change originating from a participant that does not match this field must be rejected.
If unset, then change from any participant is accepted and that participant can set this field.

``open_world`` is required. If set to true then party allocations are
required and submission from parties unknown to the ledger or a submission
in which the declared submitting party is not hosted on the submitting
participant must be rejected.

message LedgerTimeModel
^^^^^^^^^^^^^^^^^^^^^^^

*since version 1*

Defines the ledger time model, which governs the rules for acceptable
ledger effective time and maximum record time parameters that are part
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
Given the record time `RT`, a transaction submission with a ledger effective time `LET`
 and maximum record time `MRT` is accepted if:

* the difference between `LET` and `MRT` is above `MINTTL`, where
  `MINTTL = min_transaction_latency + max_clock_skew`.
* the difference between `LET` and `MRT` is below `max_ttl`.
* `RT` is between (`LET` - `max_clock_skew`) and `MRT`.

