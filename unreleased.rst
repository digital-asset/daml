.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [DAML Assistant] **BREAKING CHANGE** Changed the meaning of the ``source`` field in the daml.yaml
  file to be a pointer to the source directory of the DAML code contained in a project relative to
  the project root. This is breaking projects, where the ``source`` field of the project is pointing
  to a non-toplevel location in the source code directory structure.
