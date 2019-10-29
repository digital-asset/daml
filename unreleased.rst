.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [DAML Compiler] Reduce the memory footprint of the IDE and the command line tools (ca. 18% in our experiments).
- [DAML Triggers] Add ``dedupCreate`` and ``dedupExercise`` helpers that will only send
  commands if they are not already in flight.
