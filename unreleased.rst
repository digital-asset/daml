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
