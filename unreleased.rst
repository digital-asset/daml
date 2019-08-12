.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Ledger] Fixed a bug that prevented the ledger from loading transactions with empty workflow ids.
+ [DAML Compiler] The ``--project-root`` option now works properly
  with relative paths in ``daml build``.
+ [Ledger] Fixed internal shutdown order to avoid dead letter warnings when stopping Sandbox/Ledger API Server.
  See issue `#1886 <https://github.com/digital-asset/daml/issues/1886>`__.
+ [DAML Compiler] Support generic template declarations and instances.
+ [DAML Studio] Print stack trace when a scenario fails.
