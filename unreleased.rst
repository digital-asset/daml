.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Java Bindings] Add helper to prepare transformer for ``Bot.wire``. See `issue #3097 <https://github.com/digital-asset/daml/issues/3097>`_.
+ [Ledger] The ledger api index server starts only after the indexer has finished initializing the database.
+ [DAML Standard Library] Add ``DA.Action.State`` module containing a ``State`` action that
  can be used for computations that modify a state variable.
+ [DAML Compiler] Fixed the location of interface files when the
  ``source`` field in ``daml.yaml`` points to a file. This is mainly
  important for when you want to use the created ``.dar`` in the
  ``dependencies`` field of another package.
  See `issue #3135 <https://github.com/digital-asset/daml/issues/3135>`_.
+ [SQL Extractor] The format used for storing Optional and Map values found in contracts
  as JSON has been replaced with :doc:`/json-api/lf-value-specification`.  See `issue
  #3066 <https://github.com/digital-asset/daml/issues/3066>`_ for specifics.
+ [Scala Codegen] Fixes for StackOverflowErrors in reading large LF archives. See `issue #3104 <https://github.com/digital-asset/daml/issues/3104>`_.
+ [JSON API - Experimental] Returning archived and active/created contracts from ``/command/exercise``
  enpoint. See `issue #2925 <https://github.com/digital-asset/daml/issues/2925>`_.
+ [JSON API - Experimental] Flattening the output of the ``/contracts/search`` endpoint.
  The endpoint returns ``ActiveContract`` objects without ``GetActiveContractsResponse`` wrappers.
  See `issue #2987 <https://github.com/digital-asset/daml/pull/2987>`_.
- [DAML Assistant] ``daml start`` now supports ``--sandbox-option=opt``, ``--navigator-option=opt``
  and ``--json-api-option=opt`` to pass additional option to sandbox/navigator/json-api.
  These flags can be specified multiple times.
- [DAML Studio] ``damlc ide`` now also supports a ``--target`` option.
  The easiest way to specify this is the ``build-options`` field in ``daml.yaml``.
- [Ledger]
  Improve SQL backend performance by eliminating extra queries to the database.
- [DAML Tool - Visual]
  Adding `daml damlc visual-web` command. visual-command generates webpage with `d3 <https://d3js.org>`_ network.
+ [DAML Ledger Integration Kit] The transaction service is now fully tested.
- [DAML Compiler] Fix a problem where constraints of the form ``Template (Foo t)`` caused the compiler to suggest enabling the ``UndecidableInstances`` language extension.
- [Security] Document how to verify the signature on release tarballs.
+ [DAML Ledger Integration Kit] The TTL for commands is now read from the configuration service.
+ [DAML Ledger Integration Kit] The contract key tests now live under a single test suite and are multi-node aware.
- [DAML Compiler] **BREAKING CHANGE** Desugar template instances to ``type`` synonyms instead of ``newtype``s.
  This type definition is easier to use in the surface DAML and consistent with the types translated to DAML LF.
  The only change to user DAML is that uses of the ``TI`` ``newtype`` generated from
  ``template instance TI = T A1 .. AN`` should be simplified to use the type synonym ``type TI = T A1 .. AN``.
+ [Scala Bindings] Fixed a bug in the retry logic of ``LedgerClientBinding#retryingConfirmedCommands``. Commands are now only retried when the server responds with status ``RESOURCE_EXHAUSTED`` or ``UNAVAILABLE``.
