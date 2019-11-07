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
- [DAML Triggers] Triggers must now be compiled with ``daml build --target 1.7`` instead of ``1.dev``.
- [Ledger] Various multi-domain improvements to the ledger api server:
   * Fix divulged contract visibility in multi-participant environments. See `issue #3351 <https://github.com/digital-asset/daml/issues/3351>`__.
   * Enable the ability to configure ledger api servers with a time service (for test purposes only).
   * Allow a ledger api server to share the DAML engine with the DAML-on-X participant node for performance. See `issue #2975 <https://github.com/digital-asset/daml/issues/2975>`__.
   * Allow non-alphanumeric characters in ledger api server participant ids (space, colon, hash, slash, dot).
   * Include SQL statement type in ledger api server logging of SQL errors.