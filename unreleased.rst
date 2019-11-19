.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Stdlib] Added the ``NumericScale`` typeclass, which improves the type inference for Numeric literals, and helps catch the creation of out-of-bound Numerics earlier in the compilation process.

- [DAML Triggers] ``emitCommands`` now accepts an additional argument
  that allows you to mark contracts as pending. Those contracts will
  be automatically filtered from the result of ``getContracts`` until
  we receive the corresponding completion/transaction.
  
- [Navigator] Fixed a bug where Navigator becomes unresponsive if the ledger does not contain any DAML packages.
  
- [Ledger-API] Add field ``gen_map`` in Protobuf definition for ledger
  api values. This field is used to support generic maps, an new
  feature currently in development.  See issue
  https://github.com/digital-asset/daml/pull/3356 for more details
  about generic maps.

  The Ledger API will send no messages where this field is set, when
  using a stable version of DAML-LF.  However the addition of this
  field may cause pattern-matching exhaustive warnings in the code of
  ledger API clients. Those warnings can be safely ignored until
  GenMap is made stable in an upcoming version of DAML-LF.

- [JSON API - Experimental] CLI configuration to enable serving static content as part of the JSON API daemon:
  ``--static-content "directory=/full/path,prefix=static"``
  This configuration is NOT recommended for production deployment. See issue #2782.

- [Extractor] The app can now work against a Ledger API server that requires client authentication. See `issue #3157 <https://github.com/digital-asset/daml/issues/3157>`__.
- [DAML Script] This release contains a first version of an experimental DAML script
   feature that provides a scenario-like API that is run against an actual ledger.
- [DAML Compiler] The default DAML-LF version is now 1.7. You can
  still produce DAML-LF 1.6 by passing ``--target=1.6`` to ``daml
  build``.

- [JSON API - Experimental] The database schema has changed; if using
  ``--query-store-jdbc-config``, you must rebuild the database by adding
  ``,createSchema=true``.
  See `issue #3461 <https://github.com/digital-asset/daml/pull/3461>`_.

- [JSON API - Experimental] Terminate process immediately after creating schema. See issue #3386.
