.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Ledger] Fixed a bug that prevented the ledger from loading transactions with empty workflow ids.
+ [DAML Extension] DAML visualization from IDE
+ [DAML Compiler] The ``--project-root`` option now works properly
  with relative paths in ``daml build``.
+ [Ledger] Fixed internal shutdown order to avoid dead letter warnings when stopping Sandbox/Ledger API Server.
  See issue `#1886 <https://github.com/digital-asset/daml/issues/1886>`__.
+ [DAML Compiler] Support generic template declarations and instances.
+ [DAML Compile] The ``--dump-pom`` flag from ``damlc package`` has been removed as packaging
  has not relied on POM files for a while.
+ [DAML Studio] Print stack trace when a scenario fails.
+ [Navigator] ``{"None": {}}`` and ``{"Some": value}``, where previously accepted, are no longer supported or used for DAML ``Optional`` values.
  Instead, for simple cases, use the plain value for ``Some``, and ``null`` for ``None``.
  See issue `#2361 <https://github.com/digital-asset/daml/issues/2361>`__ for other cases.
+ [Navigator] [HTTP JSON] A new, more intuitive JSON format for DAML values is supported.
  See issue `#2361 <https://github.com/digital-asset/daml/issues/2361>`__.
