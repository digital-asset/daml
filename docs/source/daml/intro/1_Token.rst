.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

1 Basic contracts
=================

To begin with, you're going to write a very small DAML template, which represents a self-issued, non-transferable token. Because it's a minimal template, it isn't actually useful on its own - you'll make it more useful later - but it's enough that it can show you the most basic concepts:

- Transactions
- DAML Modules and Files
- Templates
- Contracts
- Signatories

.. hint::

  Remember that you can load all the code for this section into a folder ``1_Token`` by running ``daml new 1_Token --template daml-intro-1``

DAML ledger basics
------------------

Like most structures called ledgers, a DAML Ledger is just a list of *commits*. When we say *commit*, we mean the final result of when a *party* successfully *submits* a *transaction* to the ledger.

*Transaction* is a concept we'll cover in more detail through this introduction. The most basic examples are the creation and archival of a *contract*.

A contract is *active* from the point where there is a committed transaction that creates it, up to the point where there is a committed transaction that *archives* it.

.. Graphic with timeline inactive -> create -> active -> archive -> inactive

Individual contracts are *immutable* in the sense that an active contract can not be changed. You can only change the *active contract set* by creating a new contract, or archiving an old one.

DAML specifies what transactions are legal on a DAML Ledger. The rules the DAML code specifies are collectively called a *DAML model* or *contract model*.

DAML files and modules
----------------------

Each ``.daml`` file defines a *DAML Module* at the top:

.. literalinclude:: daml/daml-intro-1/Token.daml
  :language: daml
  :lines: 5

Code comments in DAML are introduced with ``--``:

.. literalinclude:: daml/daml-intro-1/Token.daml
  :language: daml
  :lines: 4-5

Templates
---------

A ``template`` defines a type of contract that can be created, and who has the right to do so. *Contracts* are instances of *templates*.

.. literalinclude:: daml/daml-intro-1/Token.daml
  :language: daml
  :lines: 12,17,21,25,30
  :caption: A simple template

You declare a template starting with the ``template`` keyword, which takes a name as an argument.

DAML is whitespace-aware and uses layout to structure *blocks*. Everything that's below the first line is indented, and thus part of the template's body.

*Contracts* contain data, referred to as the *create arguments* or simply *arguments*. The ``with`` block defines the data type of the create arguments by listing field names and their types. The single colon ``:`` means "of type", so you can read this as "template ``Token`` with a field ``owner`` of type ``Party``".

``Token`` contracts have a single field ``owner`` of type ``Party``. The fields declared in a template's ``with`` block are in scope in the rest of the template body, which is contained in a ``where`` block.

Signatories
-----------

The ``signatory`` keyword specifies the *signatories* of a contract. These are the parties whose *authority* is required to create the contract or archive it -- just like a real contract. Every contract must have at least one signatory.

Furthermore, DAML ledgers *guarantee* that parties see all transactions where their authority is used. This means that signatories of a contract are guaranteed to see the creation and archival of that contract.

Next up
-------

In :doc:`2_DamlScript`, you'll learn about how to try out the ``Token`` contract template in DAML's inbuilt DAML Script testing language.
