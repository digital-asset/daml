.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _exceptions:

Reference: Exceptions
#####################

Exceptions are a Daml feature which provides a way to handle
certain errors that arise during interpretation instead of aborting
the transaction, and to roll back the state changes that lead to the
error.

There are two types of errors:

Builtin Errors
..............

.. list-table::
   :header-rows: 1

   * - Exception type
     - Thrown on
   * - ``GeneralError``
     - Calls to ``error`` and ``abort``
   * - ``ArithmeticError``
     - Arithmetic errors like overflows and division by zero
   * - ``PreconditionFailed``
     - ``ensure`` statements that return ``False``
   * - ``AssertionFailed``
     - Failed ``assert`` calls (or other functions from ``DA.Assert``)

Note that other errors cannot be handled via exceptions, e.g., an
exercise on an inactive contract will still result in a transaction
abort.

User-defined Exceptions
.......................

Users can define their own exception types which can be thrown and
caught. The definition looks similar to templates, and just like with
templates, the definition produces a record type of the given name
as well as instances to make that type throwable and catchable.

In addition to the record fields, exceptions also need to define a
``message`` function.

.. literalinclude:: ../code-snippets-dev/Exceptions.daml
   :language: daml
   :start-after: -- BEGIN_EXCEPTION
   :end-before: -- END_EXCEPTION

Throw Exceptions
................

There are two ways to throw exceptions:

1. Inside of an ``Action`` like ``Update`` or ``Script`` you can use
   ``throw`` from ``DA.Exception``. This works for any ``Action`` that
   is an instance of ``ActionThrow``.
2. Outside of ``ActionThrow`` you can throw exceptions using ``throwPure``.

If both are an option, it is generally preferable to use ``throw``
since it is easier to reason about when exactly the exception will get
thrown.

Catch Exceptions
................

Exceptions are caught in try-catch blocks similar to those found in
languages like Java. The ``try`` block defines the scope within which
errors should be handled while the ``catch`` clauses defines which
types of errors are handled and how the program should continue. If an
exception gets caught, the subtransaction between the ``try`` and the
the point where the exception is thrown is rolled back. The actions
under rollback nodes are still validated, so, e.g., you can never
fetch a contract that is inactive at that point or have two contracts
with the same key active at the same time. However, all ledger state
changes (creates, consuming exercises) are rolled back to the state
before the rollback node.

Each try-catch block can have multiple ``catch`` clauses with the
first one that applies taking precedence.

In the example below the ``create`` of ``T`` will be rolled back and
the first ``catch`` clause applies which will create an ``Error``
contract.

.. literalinclude:: ../code-snippets-dev/Exceptions.daml
   :language: daml
   :start-after: -- BEGIN_TRY
   :end-before: -- END_TRY
