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
+ [DAML Integration Kit] Introduced initial support for multi-node testing. Note that for the time
  being no test actually uses more than one node.
+ [DAML Integration Kit] **BREAKING CHANGE** The ``-p`` / ``--target-port`` and ``-h`` / ``--host``
  flags have been discontinued. Pass one (or more) endpoints to test as command line arguments in the
  ``<host>:<port>`` form.
+ [DAML Standard Library] **BREAKING CHANGE** The ``(/)`` operator was moved out of the ``Fractional`` typeclass into a separate ``Divisible`` typeclass, which is now the parent class of `Fractional`. The ``Int`` instance of ``Fractional`` is discontinued, but there is still an ``Int`` instance of ``Divisible``. This change will break projects where a ``Fractional`` instance is defined. To fix it, add a ``Divisible`` instance and move the definition of ``(/)`` there.
+ [DAML Standard Library] **BREAKING CHANGE** The ``(/)`` operator was moved out of the ``Fractional`` typeclass into a separate ``Divisible`` typeclass, which is now the parent class of `Fractional`. The ``Int`` instance of ``Fractional`` is discontinued, but there is an ``Int`` instance of ``Divisible``. This change will break projects where a ``Fractional`` instance is defined. To fix it, add a ``Divisible`` instance and move the definition of ``(/)`` there.
+ [DAML Standard Library] **BREAKING CHANGE** The ``(/)`` operator was moved out of the ``Fractional`` typeclass into a separate ``Divisible`` typeclass, which is now the parent class of ``Fractional``. The ``Int`` instance of ``Fractional`` is discontinued, but there is an ``Int`` instance of ``Divisible``. This change will break projects where a ``Fractional`` instance is defined. To fix it, add a ``Divisible`` instance and move the definition of ``(/)`` there.
