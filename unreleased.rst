.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Java codegen] If the DAR source cannot be read, the application crashes and prints an error report.
+ [DAML Assistant] Java and Scala codegen is now integrated with the
  assistant and distributed with the SDK. It can be run via ``daml codegen``.
  You can find more information in the `DAML Assistant documentation <https://docs.daml.com/tools/assistant.html>`_.
+ [Ledger] Upgraded ledger-api server H2 Database version to 1.4.199 with stability fixes including one to the ``merge`` statement.
+ [DAML Integration Kit] One more test case added. Transaction service tests are not multi-node aware.
+ [DAML Integration Kit] Semantic tests now ensure synchronization across participants when running in a multi-node setup.
