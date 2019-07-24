.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Studio] Scenario links no longer disappear if the
  current file does not compile. The location is adjusted but this is done
  one a best effort basis and can fail if the scenario itself is modified.

- [releases] Releases should now be announced on `the releases blog
  <https://blog.daml.com/release-notes>`_.
- [DAML Compiler] Support reading of DAML-LF 1.5 again.

- [Ledger API] **BREAKING CHANGE** Drop support for legacy identifier. The
  previously deprecated field ``name`` in ``Identifier`` message is not
  supported anymore. Use ``module_name`` and ``entity_name`` instead.
- [Documentation] Improved the Maven pom.xml file for ``quickstart-java`` to better integrate with VS Code.
  See `#887 <https://github.com/digital-asset/daml/issues/887>`__.
- [Navigator] Fixed an issue when Navigator console did not see any contracts.
  See `#2271 <https://github.com/digital-asset/daml/issues/2271 >`__.
