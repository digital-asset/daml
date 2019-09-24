.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [JSON API] ``/contracts/search`` now supports a query language for filtering the
  contracts returned by matching fields.  See `issue 2778
  <https://github.com/digital-asset/daml/issues/2778>`_.
- [DAML Compiler]
  Fix a bug where ``.dar`` files produced by ``daml build`` were missing
  all ``.daml`` files except for the one that ``source`` pointed to.
+ [DAML Integration Toolkit] 30 more test cases have been added to the transaction service test suite.
- [DAML Compiler]
  Fix a bug where importing the same module from different directories
  resulted in an error in ``daml build``.
- [DAML Compiler]
  ``damlc migrate`` now produces a project that can be built with ``daml build`` as opposed to
  having to use the special ``build.sh`` and ``build.cmd`` scripts.
- [security] Starting with this one, releases are now signed on GitHub.
