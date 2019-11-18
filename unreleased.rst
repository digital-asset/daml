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
  
- [Leger-API] Add field ``gen_map`` in Protobuf definition for ledger
  api values. This field is used to support generic maps, an new
  feature currently in development.  See issue
  https://github.com/digital-asset/daml/pull/3356 for more details
  about generic maps.

  The Leger API will send no messages where this field is set, when
  using a stable version of DAML-LF.  However the addition of this
  field may cause pattern-matching exhaustive warnings in the code of
  ledger API clients. Those warnings can be safely ignored until
  GenMap is made stable in an upcoming version of DAML-LF.
