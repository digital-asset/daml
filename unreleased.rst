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
  so ``damlc ide --ghc-option -W`` launches the IDE with more warnings. Note that
  an option for the VSCode extension to pass additional options is still work in progress.
+ [DAML Docs] For ``damlc docs``, the ``--template`` argument now takes the path to a Mustache template when generating Markdown, Rst, and HTML output. The template can use ``title`` and ``body`` variables to control the appearance of the docs.
+ [DAML Assistant] Spaces in user names or other parts of file names should now be handled correctly.
+ [DAML Assistant] The ``daml deploy`` and ``daml ledger`` experimental commands were added. Use ``daml deploy --help`` and ``daml ledger --help`` to find out more about them.
+ [DAML Integration Kit] Participant State API and kvutils was extended with support for changing the ledger configuration. See changelog in respective ``package.scala`` files.
