.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Documentation] Added platform-independent tips for testing
+ [DAML Compiler] Some issues that caused ``damlc test`` to crash on shutdown have been fixed.
+ [DAML Compiler] The DAML compiler was accidentally compiled without
  optimizations on Windows. This has been fixed which should improve
  the performance of ``damlc`` and ``daml studio`` on Windows.
+ [DAML Compiler] ``damlc build`` should no longer leak file handles so
  ``ulimit`` workarounds should no longer be necessary.
+ [DAML-LF] **Breaking** Rename ``NUMERIC`` back to ``DECIMAL`` in Protobuf definition.
+ [DAML Compiler] Allow more contexts in generic templates. Specifically, template constraints can
  have arguments besides type variables, if the FlexibleContexts extension is enabled.
+ [DAML Studio] ``damlc ide`` now also accepts ``--ghc-option`` arguments like ``damlc build``
  so ``damlc ide --ghc-option -W`` launches the IDE with more warnings.
+ [DAML Integration Kit] Participant State API and kvutils was extended with support for
  changing the ledger configuration. See changelog in respective ``package.scala`` files.
+ [DAML Studio] The VSCode extension now has a configuration field for
  passing extra arguments to ``damlc ide``.
+ [Sandbox] Fixed a bug that caused the reset service to hang for 10 seconds. See issue `#2549 <https://github.com/digital-asset/daml/issues/2549>`__.
+ [Java Bindings] The Java Codegen now supports parametrized ContractIds.
  See `#2258 <https://github.com/digital-asset/daml/issues/2258>`__.
+ [DAML Standard Library] Add ``stripInfix`` function to ``DA.List``.
