.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------
- [DAML Ledger Integration Kit] Skew/LET/MRT/Config tests consolidated in a single suite.
- [JSON API - Experimental] Add ``/parties`` endpoint.
- [Sandbox] Party management fix, see `issue #3177 <https://github.com/digital-asset/daml/issues/3177>`_.
+ [Ledger] Fixed a bug where ``CreatedEvent#event_id`` field is not properly filled by ``ActiveContractsService``.
  See `issue #65 <https://github.com/digital-asset/daml/issues/65>`__.
+ [DAML-LF (Internal)] Change the name of the bintray/maven ``com.digitalasset.daml-lf-archive-scala`` to ``com.digitalasset.daml-lf-archive-reader``
+ [DAML Triggers -Experimental] The trigger runner now logs output from ``trace``, ``error`` and
  failed command completions and hides internal debugging output.
+ [Sandbox] The maximum allowed TTL for commands is now configurable via the ``--max-ttl-seconds`` parameter, for example: ``daml sandbox --max-ttl-seconds 300``.
