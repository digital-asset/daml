.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Sandbox] The sandbox now properly sets the connection pool properties ``minimumIdle``, ``maximumPoolSize``, and ``connectionTimeout``.
+ [Java codegen] Fix bug caused the generation of duplicate methods that affected sources with data constructors with type parameters that are either non-unique or not presented in the same order as in the corresponding data type declaration. See `#2367 <https://github.com/digital-asset/daml/issues/2367>`__.
