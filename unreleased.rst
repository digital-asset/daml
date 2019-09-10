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
+ [DAML Integration Kit] The test tool can now run all semantic test in a multi-node setup.
+ [DAML Assistant] The HTTP JSON API is now integrated with the
  assistant and distributed with the SDK. It can either be launched
  via ``daml json-api`` or via ``daml start``. You can find more information in the
  `README <https://github.com/digital-asset/daml/blob/master/ledger-service/http-json/README.md>`_.
+ [JSON API] **BREAKING CHANGE** The ``/contracts/search`` request payload must use
  ``"%templates"`` in place of ``"templateIds"`` to select which templates' contracts are
  returned.  See `issue #2777 <https://github.com/digital-asset/daml/issues/2777>`_.
+ [DAML Compiler] **BREAKING CHANGE** Move the DAML-LF produced by generic template instantiations closer to the surface syntax. See the documentation on `How DAML types are translated to DAML-LF <https://docs.daml.com/app-dev/daml-lf-translation.html#template-types>`__ for details.
+ [DAML Assistant] The `daml.yaml` file now supports an additional field
  `build-options`, which you can use to list cli options you want added to
  invocations of `daml build` and `daml ide`.
