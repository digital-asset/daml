.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [Ledger] Various multi-domain improvements to the ledger api server:
   * Fix divulged contract visibility in multi-participant environments. See `issue #3351 <https://github.com/digital-asset/daml/issues/3351>`__.
   * Enable the ability to configure ledger api servers with a time service (for test purposes only).
   * Allow a ledger api server to share the DAML engine with the DAML-on-X participant node for performance. See `issue #2975 <https://github.com/digital-asset/daml/issues/2975>`__.
   * Allow non-alphanumeric characters in ledger api server participant ids (space, colon, hash, slash, dot).
   * Include SQL statement type in ledger api server logging of SQL errors.