.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Adding constraints to a contract
==============================

Oftentimes, contract models will have constraints on the data stored or the allowed data transformations. In this section, you will learn about the two main mechanisms provided in DAML:

1. The ``ensure`` keyword.
2. The ``assert``, ``abort`` and ``error`` keywords.

To make sense of the latter, you'll also learn more about the ``Update`` type and ``do`` blocks, which will be good preparation for :doc:`7_Composing`, where you will use ``do`` blocks to compose choices into complex transactions.

Lastly, you will learn about time on the ledger and in scenarios.

Template Pre-Conditions
-----------------------

The first kind of restriction one commonly puts on the contract model are called *template pre-conditions*. These are simply restrictions on the data that can be stored on a contract from that template. Suppose, for examples, that the ``SimpleIou`` contract from :ref:`simple_iou` should only be able to store positive amounts. This can be enforced using the ``ensure`` keyword.

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTED_IOU_BEGIN
  :end-before: -- RESTRICTED_IOU_END

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTIONS_TEST_BEGIN
  :end-before: -- RESTRICTIONS_TEST_END

The ``ensure`` keyword takes a single expression of type ``Bool``. If you wanted to add more restrictions, you can use logical operators ``&&``, ``||`` and ``not`` to build up expressions. The above shows the additional restriction that currencies are three capital letters.

Assertions and Errors
---------------------

A second common kind of restriction is one on data transformations. For example, the simple Iou in :ref:`simple_iou` allowed the no-op where the ``owner`` transfers to themselves. This can be prevented using an ``assert`` statement, which you have already encountered in the context of scenarios. ``assert`` does not return an informative error so often it's better to use the function ``assertMsg``, which takes a custom error message:

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_CHOICE_BEGIN
  :end-before: -- TRANSFER_CHOICE_END

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_TEST_BEGIN
  :end-before: -- TRANSFER_TEST_END

In a similar vein, one can write a ``Redeem`` choice, which allows the ``owner`` to redeem an ``Iou`` during business hours on week days. The choice doesn't do anything other than archiving the ``SimpleIou``. The assumption here is that actual cash changes hands off-ledger.

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_CHOICE_BEGIN
  :end-before: -- REDEEM_CHOICE_END

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_TEST_BEGIN
  :end-before: -- REDEEM_TEST_END

There are quite a few new time-related functions here so it's time to talk about time on DAML ledgers. There's also quite a lot going on inside the ``do`` block of the ``Redeem`` choice, with several uses of the ``<-`` operator, which deserves a proper explanation at this point.

Time on DAML ledgers
--------------------

Each transaction on a DAML ledger has two timestamps called the *ledger effective time (LET)* and the *record time (RT)*. The ledger effective time is set by the submitter of a transaction, the record time is set by the consensus protocoll. Each DAML ledger has a policy on the allowed difference between LET and RT called the *skew*. The submitter has to take a good guess at what the record time will be. If it's too far off, the transaction will be rejected.

``getTime`` is an action that gets the LET from the ledger. In the above example, that time is taken apart into day of week and hour of day using standard library functions from ``DA.Date`` and ``DA.Time``. The hour of the day is checked to be in the range from 8 to 18. Suppose now that the ledger had a skew of 10 seconds, but a submission took less than 4 seconds to commit. At 18:00:05, Alice could submit a transaction to redeem an Iou with a LET of 17:59:59. It would be a valid trasnaction and be committed successfully as ``hrs == 17`` and ``LET - RT < 10 seconds``. Time therefore has to be considered slightly fuzzy in DAML, with the fuzzyness depending on the skew parameter.

In scenarios, record and ledger effective time are always equal and they can be set using the functions ``passToDate`` and ``pass``. ``passToDate`` takes a date sets the time to midnight (UTC) of that date. ``pass`` takes a ``Reltime``, a relative time and moves the ledger by that much.

On a distributed DAML ledger, there are no guarantees that ledger effective time or relative time are strictly increasing. The only guarantee is that ledger effective time is increasing with causality. If a transaction ``TX2`` depends on a transaction ``TX1``, then the ledger enforces that the LET of ``TX2`` is greater than or equal to that of ``TX1``:

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- CAUSALITY_TEST_BEGIN
  :end-before: -- CAUSALITY_TEST_END

Actions and ``do`` blocks
-------------------------

You have come across ``do`` blocks and ``<-`` notations in two contexts by now: ``Scenario`` and ``Update``. Both of these are examples of an ``Action``, also called a *Monad* in functional programming.

Expressions in DAML are pure in the sense that they have no side-effects. They neither read nor modify any external state. If you know the value of all variables in scope and write an expression, you can work out the value of that expression on pen and paper. All the expressions you've seen that used the ``<-`` notation were not like that.

``getTime`` is a good example. There is nothing in scope that could inform the value of ``now`` so there is no expression ``expr`` that you could put on the right hand side of ``now = expr``. To get the ledger effective time, you need to be in the context of a submitted transaction and then look at that context.

Similarly, you've come across ``fetch``. If you have ``cid : ContractId Account`` in scope and you come across the expression ``fetch cid``, you can't evaluate that to an ``Account`` so you can't write ``account = fetch cid``. To do so, you'd have to have a ledger on scope on which to look up that contract id.

Actions are a way to handle such "impure" expressions. ``Action a`` is a type class with a single parameter ``a`` and ``Update`` and ``Scenario`` are instances of ``Action``. A value of such a type ``m a`` where ``m`` is an instance of ``Action`` can be interpreted as "a recipe for an action of type ``m``, which, when executed, returns a value ``a``".

- An ``Update a`` is "a recipe to update a DAML ledger, which, when committed, returns a value of type ``a``". An update to a DAML ledger is a transaction so equivalently, an ``Update a`` is "a recipe to construct a transaction, which, when executed in the context of a ledger, returns a value of type ``a``".
- A ``Scenario a`` is "a recipe to submit transactions to a test ledger, which, when performed against a ledger, returns a value of type ``a``".

Expressions like ``getTime``, ``getParty``, ``pass``, ``submit``, ``create`` and ``exercise`` should make more sense in that light. For example:

- ``getTime : Update Time`` is the recipe for an empty transaction that also happens to return a value of type ``Time``.
- ``pass (days 10) : Scenario ()`` is a recipe for a transaction that doesn't submit any transactions, but has the side-effect of changing the LET of the test ledger. It returns ``()``, also called ``Unit`` and can be thought of as a zero-tuple.
- ``create iou : Update (ContractId Iou)``, where ``iou : Iou`` is a recipe for a transaction consisting of a single ``create`` action, and returns the contract id of the created contract if successful.
- ``submit alice (create iou) : Scenario (ContractId Iou)`` is a recipe for a scenario in which Alice evaluates the result of ``create iou`` to get a transaction and a return value of type ``ContractId Iou``, and then submits that transaction to the ledger.

Any DAML ledger knows how to perform actions of type ``Update a``. Only some know how to run scenarions, meaning they can perform actions of type ``Scenario a``.

A transacion is a list of actions so a transaction followed by another transaction is again a transaction. Similarly, a scenario is a list of interactions with the ledger (``submit``, ``getParty``, ``pass``, etc.) so a scenario followed by another scenario is again a scenario. This is where ``do`` blocks come in.
