.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Stdlib] Added the ``NumericScale`` typeclass, which improves the type inference for Numeric literals, and helps catch the creation of out-of-bound Numerics earlier in the compilation process.

- [Leger-API] Add field ``gen_map`` in Protobuf definition for ledger
  api values. This field is used to support generic maps, an upcoming
  feature currently in development.  See issue
  https://github.com/digital-asset/daml/pull/3356 for more details
  about generic maps.

  Leger API clients will receive no messages where this field is set,
  if using a stable version of DAML-LF.  The addition of this field
  may cause pattern-matching exhaustive warnings that can be safely
  ignored until futher notice.

