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
- [DAML Compiler] Support for incremental builds in ``daml build`` using the ``--incremental=yes`` flag.
  This is still experimental and disabled by default but will become enabled by default in the future.
  On large codebases, this can significantly improve compile times and reduce memory usage.
- [DAML Compiler] Support for data dependencies on packages compiled with an older SDK
  (experimental). To import data dependencies, list the packages under the ``data-dependencies``
  stanza in the project's daml.yaml file.
- [JSON API - Experimental] Fix to support Archive choice. See issue #3219
- [Sandbox] Add the option to start the sandbox with JWT based authentication. See `issue #3363 <https://github.com/digital-asset/daml/issues/3363>`__.
- [Sandbox] Fixed a bug in the SQL backend that caused the database to be flooded with requests when streaming out transactions.
- [Extractor - Experimental] Fix bug in reading TLS parameters.
- [DAML Stdlib] Add a ``maintainer`` function that will give you the list of maintainers of a contract key.
- [DAML Triggers] Added ``exerciseByKeyCmd`` and
  ``dedupExerciseByKey`` to exercise a choice given the contract key
  instead of the contract id.
- [DAML Triggers] ``getTemplates`` has been renamed to ``getContracts`` to describe its behavior more accurately.
  ``getTemplates`` still exists as a compatiblity helper but it is deprecated and will be removed in a future SDK release.
- [JSON API - Experimental] Fix to support Archive choice. See issue #3219.
- [JSON API - Experimental] Implement replay on database consistency violation, See issue #3387.
- [JSON API - Experimental] Comparison/range queries supported.
  See `issue #2780 <https://github.com/digital-asset/daml/issues/2780>`__.
