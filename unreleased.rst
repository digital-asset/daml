.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------
- [DAML Triggers] The trigger runner now supports triggers using the high-level API directly. These no longer need to be converted to low-level Triggers using ``runTrigger``. Triggers using the low-level API are still supported.
- [DAML Compiler] The package database is now be cleaned automatically on initialization.
  This means that you should no longer have to run ``daml clean`` on SDK upgrades
  if you use DAR dependencies (e.g. with DAML triggers).
