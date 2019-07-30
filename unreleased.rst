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
+ [Sandbox] Updated the PostgreSQL JDBC driver to version 42.2.6.
+ [Sandbox] Added TRACE level debugging for database operations.
+ [Ledger Api] *BREAKING CHANGE** In Protobuf ``Value`` message, rename ``decimal` field to ``numeric``.
+ [Sandbox] Fixed a bug that could lead to an inconsistent snapshot of active contracts being served
  by the ActiveContractsService under high load.
+ [Sandbox] Commands are now deduplicated based on ``(submitter, application_id, command_id)``.
+ [Sandbox] Reading transactions is now more efficient. See issue `#1774 <https://github.com/digital-asset/daml/issues/1774>`__.
