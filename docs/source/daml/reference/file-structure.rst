.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: DAML file structure
##############################

This page gives reference information on the structure of DAML files outside of :doc:`templates <templates>`:

.. contents:: :local:

File structure
**************

- Language version (``daml 1.2``).
- This file's module name (``module NameOfThisFile where``).

  Part of a hierarchical module system to facilitate code reuse. Must be the same as the DAML file name, without the file extension.

  For a file with path ``./Scenarios/Demo.daml``, use ``module Scenarios.Demo where``.

.. _daml-ref-imports:

Imports
*******

- You can import other modules (``import OtherModuleName``), including qualified imports (``import qualified AndYetOtherModuleName``, ``import qualified AndYetOtherModuleName as Signifier``). Can't have circular import references.
- To import the ``Prelude`` module of ``./Prelude.daml``, use ``import Prelude``.
- To import a module of ``./Scenarios/Demo.daml``, use ``import Scenarios.Demo``.
- If you leave out ``qualified``, and a module alias is specified, top-level declarations of the imported module are imported into the module's namespace as well as the namespace specified by the given alias.

Libraries
*********

A DAML library is a collection of related DAML modules.

Define a DAML library using a ``LibraryModules.daml`` file: a normal DAML file that imports the root modules of the library. The library consists of the ``LibraryModules.daml`` file and all its dependencies, found by recursively following the imports of each module.

Errors are reported in DAML Studio on a per-library basis. This means that breaking changes on shared DAML modules are displayed even when the files are not explicitly open.

Comments
********

Use  ``--`` for a single line comment. Use ``{-`` and ``-}`` for a comment extending over multiple lines.

.. _daml-ref_contract-identifiers:

Contract identifiers
********************

When an instance of a template (that is, a contract) is added to the ledger, it's assigned a unique identifier, of type ``ContractId <name of template>``.

The runtime representation of these identifiers depends on the execution
environment: a contract identifier from the Sandbox may look different to ones on other DAML Ledgers.

You can use ``==`` and ``/=`` on contract identifiers of the same type.
