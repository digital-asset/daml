.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _testing-using-scenarios:

Testing using scenarios
#######################

DAML has a built-in mechanism for testing templates called *scenarios*.

Scenarios emulate the ledger. You can specify a linear sequence of actions that various parties take, and these are evaluated in order, according to the same consistency, authorization, and privacy rules as they would be on the sandbox ledger or ledger server. :doc:`DAML Studio </daml/daml-studio>` shows you the resulting Transaction graph.

For more on how scenarios work, see the :ref:`daml-testing-examples` below.

Scenario syntax
***************

Scenarios
=========

.. literalinclude:: code-snippets/Payout.daml
   :language: daml
   :lines: 24-25

A ``scenario`` emulates the ledger, in order to test that a DAML template or sequence of templates are working as they should.

It consists of a sequence of transactions to be submitted to the ledger (after ``do``), together with success or failure assertions.

Transaction submission
======================

.. literalinclude:: code-snippets/Payout.daml
   :language: daml
   :lines: 28-29
   :dedent: 4

The :ref:`submit <daml-ref-commits>` function attempts to submit a transaction to the ledger on behalf of a ``Party``.

For example, a transaction could be :ref:`creating <daml-ref-create>` a contract on the ledger, or :ref:`exercising <daml-ref-exercise>` a choice on an existing contract.

Asserting transaction failure
=============================

.. literalinclude:: code-snippets/CallablePayout_Call.daml
   :language: daml
   :lines: 72-73
   :dedent: 4

The :ref:`submitMustFail <daml-ref-fails>` function asserts that submitting a transaction to the ledger would fail.

This is essentially the same as ``submit``, except that the scenario tests that the action doesn't work.

Full syntax
===========

For detailed syntax, see :doc:`reference/scenarios`.

Running scenarios in DAML Studio
********************************

When you load a file that includes scenarios into :doc:`DAML Studio </daml/daml-studio>`, it displays a "Scenario results" link above the scenario. Click the link to see a representation of the ledger after the scenario has run.

.. _daml-testing-examples:

Examples
********

Simple example
==============

A very simple scenario looks like this:

.. literalinclude:: code-snippets/Payout.daml
   :language: daml
   :lines: 24-32

In this example, there is only one transaction, authorized by the party ``Alice`` (created using ``getParty "Alice"``).  The ledger update is a ``create``, and has to include the :ref:`arguments for the template <daml-ref-template-parameters>` (``Payout with receiver = alice; giver = alice``).

.. Download :download:`the Payout template <code-snipppets/Payout.daml>`.

Example with two updates
========================

This example tests a contract that gives both parties an explicit opportunity to agree to their obligations.

.. literalinclude:: code-snippets/CallablePayout_Call.daml
   :language: daml
   :lines: 33-45

In the first transaction of the scenario, party ``bankOfEngland`` (created using ``getParty "Bank of England"``) creates a ``CallablePayout`` contract with ``alice`` as the receiver and ``bankOfEngland`` as the giver.

When the contract is submitted to the ledger, it is given a unique contract identifier of type ``ContractId CallablePayout``. ``payAlice <-`` assigns that identifier to the variable ``payAlice``.

In the second statement, ``exercise payAlice Call``, is an exercise of the ``Call`` choice on the contract identified by ``payAlice``. This causes a ``Payout`` agreement with her as the ``receiver`` to be written to the ledger.

The workflow described by the above scenario models both parties explicitly exercising their rights and accepting their obligations:

- Party ``"Bank of England"`` is assumed to know the definition of the ``CallablePayout`` contract template and the consequences of submitting a contract to the ledger.
- Party ``"Alice"`` is assumed to know the definition of the contract template, as well as the consequences of exercising the ``Call`` choice on it. If ``"Alice"`` does not want to receive five pounds, she can simply not exercise that choice.

Example with submitMustFail
===========================

Because exercising a contract (by default) archives a contract, once party ``"Alice"`` exercises the ``Call`` choice, she will be unable to exercise it again.

To test this expectation, use the ``submitMustFail`` function:

.. literalinclude:: code-snippets/CallablePayout_Call.daml
   :language: daml
   :lines: 56-73

When the ``Call`` choice is exercised, the contract is archived. The ``fails`` keyword checks that if ``'Alice'`` submits ``exercise payAlice Call`` again, it would fail.
