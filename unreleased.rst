.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Compiler] Reduce the memory footprint of the IDE and the command line tools (ca. 18% in our experiments).
- [DAML Triggers] Add ``dedupCreate`` and ``dedupExercise`` helpers that will only send
  commands if they are not already in flight.
- [Sandbox] Fixed a bug a database migration script for Sandbox on Postgres introduced in SDK 0.13.32. See `issue #3284 <https://github.com/digital-asset/daml/issues/3284>`__.
- [DAML Integration Kit] Re-add :doc:`integration kit documentation </daml-integration-kit/index>` that got accidentally deleted.
- [Ledger API] Disallow empty commands. See `issue #592 <https://github.com/digital-asset/daml/issues/592>`__.