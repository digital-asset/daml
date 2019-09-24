.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

DAML Tool - Visual
~~~~~~~~~~~~~~~~~~

- Adding `daml damlc visual-web` command. visual-command generates webpage with `d3 <https://d3js.org>`_ network.
- [DAML Assistant] ``daml start`` now supports ``--sandbox-option=opt``, ``--navigator-option=opt``
  and ``--json-api-option=opt`` to pass additional option to sandbox/navigator/json-api.
  These flags can be specified multiple times.
- [DAML Compiler] Fix a bug where generic templates could crash the compiler.
