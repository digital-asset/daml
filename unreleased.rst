.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML-LF] Freeze DAML-LF 1.7. Summary of changes (See DAML-LF specification for more details.):
   * Add support for parametrically scaled Numeric type.
   * Drop support of Decimal in favor or Numerics.
   * Add interning of strings and names. This reduces drastically dar file size.
   * Add support for 'Any' type.
   * Add support for type representation values.
 - [DAML-LF] Add immutable bintray/maven packages for handling DAML-LF archive up to version 1.7:
   + `com.digitalasset.daml-lf-1.7-archive-proto`

     This package contains the archive protobuf definitions as they
     were introduced when 1.7 was frozen.  These definitions can be
     used to read DAML-LF archives up to version 1.7.
