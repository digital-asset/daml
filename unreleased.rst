.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML-LF] Freeze DAML-LF 1.7. Summary of changes (See DAML-LF specification for more details.):
   * Add support for parametrically scaled Numeric type.
   * Drop support of Decimal in favor or Numerics.
   * Add interning of strings and names. This reduces drastically dar file size.
   * Add support for 'Any' type.
   * Add support for type representation values.
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
- [DAML Triggers] Remove the custom ``AbsoluteContractId`` type in favor of the regular ``ContractId`` type used in DAML templates.
- [Ledger] Fixed divulged contract visibility in multi-participant environments. See `issue #3351 <https://github.com/digital-asset/daml/issues/3351>`__.
- [Ledger] Enabled the ability to configure ledger api servers with a time service (for test purposes only).
