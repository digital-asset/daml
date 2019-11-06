.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [Navigator] Fixed regression in Navigator to properly respect the CLI option ``--ledger-api-inbound-message-size-max`` again. See `issue #3301 <https://github.com/digital-asset/daml/issues/3301>`__.
- [DAML Compiler] Reduce the memory footprint of the IDE and the command line tools (ca. 18% in our experiments).
- [DAML Triggers] Add ``dedupCreate`` and ``dedupExercise`` helpers that will only send
  commands if they are not already in flight.
- [Sandbox] Fixed a bug a database migration script for Sandbox on Postgres introduced in SDK 0.13.32. See `issue #3284 <https://github.com/digital-asset/daml/issues/3284>`__.
- [DAML Integration Kit] Re-add :doc:`integration kit documentation </daml-integration-kit/index>` that got accidentally deleted.
- [Ledger API] Disallow empty commands. See `issue #592 <https://github.com/digital-asset/daml/issues/592>`__.
- [DAML Stdlib] Add `DA.TextMap.filter` and `DA.Next.Map.filter`.
- [DAML Stdlib] Add `assertEq` and `assertNotEq` to `DA.Assert` as synonyms for `===` and `=/=`.
- [Extractor - Experimental] Extractor now stores exercise events in the single table data format. See `issue #3274 <https://github.com/digital-asset/daml/issues/3274>`__.
- [DAML Compiler] Fix compile error caused by instantiating generic
  templates at ``Numeric n``.
- [Sandbox] Timing about database operations are now exposed over JMX as well as via the logs.
- [JSON API - Experimental] ``workflowId`` no longer included in any responses.
- [JSON API - Experimental] ``/contracts/search`` endpoint can optionally store searched
  contracts in a Postgres-based cache, by passing the new ``--query-store-jdbc-config`` option.
  See `issue #2781 <https://github.com/digital-asset/daml/issues/2781>`_.
- [DAML Triggers] Remove the custom ``AbsoluteContractId`` type in favor of the regular ``ContractId`` type used in DAML templates.
- [Sandbox] Added a missing index to the SQL schema for the Postgres Ledger.
- [DAML Stdlib] Add ``DA.Foldable.mapA_``, ``DA.Foldable.forA_``, ``DA.Foldable.sequence_`` and ``DA.Action.replicateA_``. These functions
  match the behavior of corresponding functions without the underscore suffix but ignore the result which can be more convenient and
  efficient.
- [DAML Compiler] The compiler now accepts single-constructor enum types. For example ``data A = A`` or ``data Foo = Bar``.
