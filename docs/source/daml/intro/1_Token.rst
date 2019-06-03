.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

A Token Contract
================

The first DAML model you will write is a pretty useless one -- a self-issued, non-transferrable token. It represents the minimal possible DAML model and as such serves as an illustration of the most basic concepts:

- Transactions
- DAML Modules and Files
- Templates
- Contracts
- Signatories

For this section, create a new file called ``Intro_1_Token.daml`` and open it in DAML Studio. If you have installed the DAML SDK, you can start it with ``daml studio`` from a terminal or just open VS Code. Alternatively, use the :download:`supplied source file <daml/Intro_1_Token.daml>`.

DAML Ledger Basics
------------------

Like most structures called ledgers, a DAML Ledger is a list of *commits*. A *commit* is the final result of the *submission* of a *transaction* by a *party*. The most basic *transactions* are the creation and archival of a *contract*. A contract is *active* on the ledger, if there is a committed transaction that creates it, but there is no commit that archives it again.

.. Graphic with timeline inactive -> create -> active -> archive -> inactive

DAML Files and Modules
----------------------

Each `.daml` file defines a *DAML Module*. At the top of each DAML file is a pragma informing the compiler of the language version and the module name:

.. literalinclude:: daml/Intro_1_Token.daml
  :language: daml
  :lines: 3, 6

Code comments in DAML are introduced with `--`:

.. literalinclude:: daml/Intro_1_Token.daml
  :language: daml
  :lines: 1-6

Templates
---------

The type of a contract that can be created, and who has the right to do so, is defined in a ``template``. *Contracts* are instances of *templates*.

.. literalinclude:: daml/Intro_1_Token.daml
  :language: daml
  :lines: 13,18,22,26,31
  :caption: A Token template

Templates are declared starting with the ``template`` keyword and take a name as an argument. DAML is whitespace-aware and uses layout to structure *blocks*. Everything that's below the first line is indented, and thus part of the template's body.

*Contracts* on a DAML ledger contain data, often referred to as the *create arguments* or simply *arguments*. The ``with`` block starting in the second line defines the data type of the create arguments of a contract of type ``Token`` by listing field names and their types. The single colon ``:`` is read as "of type" so we can read the first three lines as "template Token with field owner of type ``Party``. ``Token`` contracts have a single field ``owner`` of type ``Party``. The fields declared in a template's ``with`` block are in scope in the rest of the template body, which is contained in a ``where`` block.

Signatories
-----------

The ``signatory`` keyword specifies the *signatories* of a contract instance. Every contract must have at least one signatory. The *authority* of all the signatories of a contract are required to create or archive it again -- just like a real contract. Furthermore, DAML ledgers *guarantee* that parties see all transactions where their authority is used, which means that signatories of a contract are guaranteed to see the creation and archival of that contract.

Next Up
-------

In :doc:`2_Scenario`, you'll learn about how to try out the ``Token`` contract template in DAML's inbuilt ``scenario`` testing language.

