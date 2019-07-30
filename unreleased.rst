.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Docs] The ``damlc docs`` command now produces docs to a folder by default. Use the new ``--combine`` flag to output a single file instead.
- [DAML Docs] The ``damlc docs`` flag ``--prefix`` has been replaced with a ``--template`` flag which allows for a more flexible template.
- [DAML Docs] The ``damlc docs`` flag ``--json`` has been dropped in favor of ``--format=json``.
- [Java Bindings]: Add all packages of java bindings to the javadocs.
  See `#2280 <https://github.com/digital-asset/daml/issues/2280>`__.
- [Sandbox] Makes package uploads idempotent and tolerate partial duplicates
  See `#2130 <https://github.com/digital-asset/daml/issues/2130>`__.
- [Ledger API, Java Bindings] **BREAKING CHANGE** Removed field ``ExercisedEvent#contract_creating_event_id``.
  See `#2068 <https://github.com/digital-asset/daml/issues/2068>`__.
- [Extractor] **BREAKING CHANGE** Changed schema to accomodate removed field ``ExercisedEvent#contract_creating_event_id``.
  Existing database schemas are not compatible anymore with the newer version. The extractor needs to be run on an empty schema from Ledger Begin.
- [Sandbox] The active contract service correctly serves stakeholders. See `#2070 <https://github.com/digital-asset/daml/issues/2070>`__.
