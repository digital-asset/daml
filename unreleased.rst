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
+ [DAML Standard Library] **BREAKING CHANGE** The ``(/)`` operator was moved out of the ``Fractional`` typeclass into a separate ``Divisible`` typeclass, which is now the parent class of ``Fractional``. The ``Int`` instance of ``Fractional`` is discontinued, but there is an ``Int`` instance of ``Divisible``. This change will break projects that rely on the ``Fractional Int`` instance. To fix that, change the code to rely on ``Divisible Int`` instead. This change will also break projects where a ``Fractional`` instance is defined. To fix that, add a ``Divisible`` instance and move the definition of ``(/)`` there.
