.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Assistant] Added ``--install-assistant`` flag to ``daml install`` command,
  changing the default behavior of ``daml install`` to install the assistant
  whenever we are installing a newer version of the SDK. Deprecated the
  ``--activate`` flag.
- [DAML Studio] Opening an already open scenario will now focus it rather than opening
  it in a new empty tab which is never updated with results.
- [DAML Studio] The selected view for scenario results (table or transaction) is now
  preserved when the scenario results are updated.
  See `#1675 <https://github.com/digital-asset/daml/issues/1675>`__.
- [DAML-LF] Rename ``TO_TEXT_CODE_POINTS`` and ``FROM_TEXT_CODE_POINTS`` to ``TEXT_FROM_CODE_POINTS`` and ``TEXT_TO_CODE_POINTS``, resp.
- [Dependencies] Protobuf has been upgraded to version 3.8.0. This
  also includes the protobuf-java library used as a dependency.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- Added additional Ledger API integration tests to Ledger API Test Tool.
- [DAML Studio] Goto definition now works on the export list of modules.
- [DAML-LF] Rename ``none`` and ``some`` to ``optional_none`` and ``optional_some``, resp., in ``Expr`` and ``CasePat``.
