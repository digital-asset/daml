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
+ [DAML Assistant] The HTTP JSON API is now integrated with the
  assistant and distributed with the SDK. It can either be launched
  via ``daml json-api`` or by passing ``--json-api-port 7575`` to
  ``daml start``. You can find more information in the
  `README <https://github.com/digital-asset/daml/blob/master/ledger-service/http-json/README.md>`_.
