.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [Sandbox] Added `--log-level` command line flag.
- [Ledger API] Added new CLI flags ``--stable-party-identifiers`` and
  ``--stable-command-identifiers`` to the :doc:`Ledger API Test Tool
  </tools/ledger-api-test-tool/index>` to allow disabling randomization of party
  and command identifiers. It is useful for testing of ledgers which are
  configured with a predefined static set of parties.
- [Sandbox] Fixed an issue when CompletionService returns offsets having inclusive semantics when used for re-subscription. 
  See `#1932 <https://github.com/digital-asset/daml/pull/1932>`__.