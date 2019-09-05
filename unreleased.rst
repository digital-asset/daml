.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [DAML Integration Kit] The reference implementation can now spin up multiple nodes, either scaling
  a single participant horizontally or adding new participants. Check the CLI ``--help`` option.
+ [DAML Integration Kit] The test tool now runs the double spend test on a shared contract in a
  multi-node setup (as well as single-node).
+ [DAML Integration Kit] The test tool can now run all semantic test in a multi-node setup.
+ [DAML Compiler] **BREAKING CHANGE** Move the DAML-LF produced by generic template instantiations closer to the surface syntax. See the documentation on `How DAML types are translated to DAML-LF <https://docs.daml.com/app-dev/daml-lf-translation.html#template-types>`__ for details.
