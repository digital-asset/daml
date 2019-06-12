.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

5 Adding constraints to a contract
==================================

Oftentimes, contract models will have constraints on the data stored, or the allowed data transformations. In this section, you will learn about the two main mechanisms provided in DAML:

1. The ``ensure`` keyword.
2. The ``assert``, ``abort`` and ``error`` keywords.

To make sense of the latter, you'll also learn more about the ``Update`` and ``Scenario`` types and ``do`` blocks, which will be good preparation for :doc:`7_Composing`, where you will use ``do`` blocks to compose choices into complex transactions.

Lastly, you will learn about time on the ledger and in scenarios.

Template preconditions
----------------------

The first kind of restriction you may want to put on the contract model are called *template pre-conditions*. These are simply restrictions on the data that can be stored on a contract from that template. Suppose, for examples, that the ``SimpleIou`` contract from :ref:`simple_iou` should only be able to store positive amounts. This can be enforced using the ``ensure`` keyword.

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTED_IOU_BEGIN
  :end-before: &&

The ``ensure`` keyword takes a single expression of type ``Bool``. If you wanted to add more restrictions, you can use logical operators ``&&``, ``||`` and ``not`` to build up expressions. The below shows the additional restriction that currencies are three capital letters.


.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: ensure
  :end-before: -- RESTRICTED_IOU_END


.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTIONS_TEST_BEGIN
  :end-before: -- RESTRICTIONS_TEST_END


Assertions and Errors
---------------------

A second common kind of restriction is one on data transformations. For example, the simple Iou in :ref:`simple_iou` allowed the no-op where the ``owner`` transfers to themselves. You can prevent that using an ``assert`` statement, which you have already encountered in the context of scenarios. ``assert`` does not return an informative error so often it's better to use the function ``assertMsg``, which takes a custom error message:

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_CHOICE_BEGIN
  :end-before: -- TRANSFER_CHOICE_END

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_TEST_BEGIN
  :end-before: -- TRANSFER_TEST_END

In a similar vein, yu can write a ``Redeem`` choice, which allows the ``owner`` to redeem an ``Iou`` during business hours on week days. The choice doesn't do anything other than archiving the ``SimpleIou``. The assumption here is that actual cash changes hands off-ledger.

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_CHOICE_BEGIN
  :end-before: -- REDEEM_CHOICE_END

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_TEST_BEGIN
  :end-before: -- REDEEM_TEST_END

There are quite a few new time-related functions from the `DA.Time` and `DA.Date` libraries here. Their names should be reasonably descriptive so they won't be covered here, but given that you have to assume that DAML is run in a distributed setting, we will still discuss time in DAML.

There's also quite a lot going on inside the ``do`` block of the ``Redeem`` choice, with several uses of the ``<-`` operator. ``do`` blocks and ``<-`` deserve a proper explanation at this point.

Time on DAML ledgers
--------------------

Each transaction on a DAML ledger has two timestamps called the *ledger effective time (LET)* and the *record time (RT)*. The ledger effective time is set by the submitter of a transaction, the record time is set by the consensus protocol. Each DAML ledger has a policy on the allowed difference between LET and RT called the *skew*. The submitter has to take a good guess at what the record time will be. If it's too far off, the transaction will be rejected.

``getTime`` is an action that gets the LET from the ledger. In the above example, that time is taken apart into day of week and hour of day using standard library functions from ``DA.Date`` and ``DA.Time``. The hour of the day is checked to be in the range from 8 to 18. Suppose now that the ledger had a skew of 10 seconds, but a submission took less than 4 seconds to commit. At 18:00:05, Alice could submit a transaction with a LET of 17:59:59 to redeem an Iou. It would be a valid trasnaction and be committed successfully as ``getTime`` will return 17:59:59 so ``hrs == 17``. Since RT will be before 18:00:09, ``LET - RT < 10 seconds`` and the transaction won't be rejected. Time therefore has to be considered slightly fuzzy in DAML, with the fuzzyness depending on the skew parameter.

Time in scenarios
~~~~~~~~~~~~~~~~~

In scenarios, record and ledger effective time are always equal. You can set them using the following functions:

- ``passToDate``, which takes a date sets the time to midnight (UTC) of that date and ``pass``
- ``pass``, which takes a ``Reltime`` (a relative time) and moves the ledger by that much

Time on ledgers
~~~~~~~~~~~~~~~~~

On a distributed DAML ledger, there are no guarantees that ledger effective time or relative time are strictly increasing. The only guarantee is that ledger effective time is increasing with causality. That is, if a transaction ``TX2`` depends on a transaction ``TX1``, then the ledger enforces that the LET of ``TX2`` is greater than or equal to that of ``TX1``:

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- CAUSALITY_TEST_BEGIN
  :end-before: -- CAUSALITY_TEST_END

Actions and ``do`` blocks
-------------------------

You have come across ``do`` blocks and ``<-`` notations in two contexts by now: ``Scenario`` and ``Update``. Both of these are examples of an ``Action``, also called a *Monad* in functional programming. ``Actions`` can be constructed in a convenient way using ``do`` notation. Understanding ``Actions`` and ``do`` blocks is therefore crucial to being able to construct correct contract models and test.

Expressions in DAML are pure in the sense that they have no side-effects. They neither read nor modify any external state. If you know the value of all variables in scope and write an expression, you can work out the value of that expression on pen and paper. The expressions you've seen that used the ``<-`` notation are not like that.

``getTime`` is a good example of an ``Action``. Here's the example we used earlier

.. code-block:: daml

   now <- getTime

You cannot work out the value of ``now`` based on any variable in scope. To put it another way, there is no expression ``expr`` that you could put on the right hand side of ``now = expr``. To get the ledger effective time, you must be in the context of a submitted transaction, and then look at that context.

Similarly, you've come across ``fetch``. If you have ``cid : ContractId Account`` in scope and you come across the expression ``fetch cid``, you can't evaluate that to an ``Account`` so you can't write ``account = fetch cid``. To do so, you'd have to have a ledger you can look that contract id up on.

Actions are a way to handle such "impure" expressions. ``Action a`` is a type class with a single parameter ``a``, and ``Update`` and ``Scenario`` are instances of ``Action``. A value of such a type ``m a`` where ``m`` is an instance of ``Action`` can be interpreted as "a recipe for an action of type ``m``, which, when executed, returns a value ``a``". You can always write a recipe using just pen and paper, but you can't cook it up unless you are in the context of a kitchen with the right ingredients and utensils.

- An ``Update a`` is "a recipe to update a DAML ledger, which, when committed, returns a value of type ``a``". An update to a DAML ledger is a transaction so equivalently, an ``Update a`` is "a recipe to construct a transaction, which, when executed in the context of a ledger, returns a value of type ``a``".
- A ``Scenario a`` is "a recipe for a test, which, when performed against a ledger, returns a value of type ``a``".

Expressions like ``getTime``, ``getParty party``, ``pass time``, ``submit party update``, ``create contract`` and ``exercise choice`` should make more sense in that light. For example:

- ``getTime : Update Time`` is the recipe for an empty transaction that also happens to return a value of type ``Time``.
- ``pass (days 10) : Scenario ()`` is a recipe for a transaction that doesn't submit any transactions, but has the side-effect of changing the LET of the test ledger. It returns ``()``, also called ``Unit`` and can be thought of as a zero-tuple.
- ``create iou : Update (ContractId Iou)``, where ``iou : Iou`` is a recipe for a transaction consisting of a single ``create`` action, and returns the contract id of the created contract if successful.
- ``submit alice (create iou) : Scenario (ContractId Iou)`` is a recipe for a scenario in which Alice evaluates the result of ``create iou`` to get a transaction and a return value of type ``ContractId Iou``, and then submits that transaction to the ledger.

Any DAML ledger knows how to perform actions of type ``Update a``. Only some know how to run scenarions, meaning they can perform actions of type ``Scenario a``.

An action followed by another action, possibly depending on the result of the first action, is just another action. Specifically:

- A transacion is a list of actions so a transaction followed by another transaction is again a transaction.
- A scenario is a list of interactions with the ledger (``submit``, ``getParty``, ``pass``, etc.) so a scenario followed by another scenario is again a scenario.

This is where ``do`` blocks come in. ``do`` blocks allow you to combine small actions in to bigger actions, using the results of earlier actions in later ones.

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- DO_DEMO_BEGIN
  :end-before: -- DO_DEMO_END

Above, we see ``do`` blocks in action for both ``Scenario`` and ``Update``.

You may already have noticed the use of ``return`` in the redeem choice. ``return x`` is a no-op action which returns value ``x`` so ``return 42 : Update Int``. Since ``do`` blocks always return the value of their last action, ``sub_scenario2 : Scenario Int``.

Failing Actions
---------------

Not only are ``Update`` and ``Scenario`` examples of ``Action``, they are both examples of actions that can fail, e.g. because a transaction is illegal or the party retrieved via ``getParty`` doesn't exist on the ledger.

Each has a special action ``abort txt`` which takes on type ``Update ()`` or ``Scenario ()`` depending on context and represents failure. Transactions and scenarios succeed or fail *atomically* as a whole so an occurrence of an ``abort`` action will always fail the entire evaluation of the current ``Scenario`` or ``Update``.

The last expression in the ``do`` block of the ``Redeem`` choice is a pattern matching expression on ``dow``. It has type ``Update ()`` and is either an ``abort`` or ``return`` depending on the day of week. So during the week, it's a no-op and on weekends, it's the special failure action. Thanks to the atomicity of transactions, no transaction can ever make use of the ``Redeem`` choice on weekends.

A sample Action
---------------

If the above didn't make complete sense, here's another example. ``CoinGame a`` is an ``Action a`` in which a ``Coin`` is flipped and based on the ``Heads`` and ``Tails`` results, a value of type ``a`` is calulated.

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- COIN_BEGIN
  :end-before: -- COIN_END

A ``CoinGame a`` exposes a function ``play`` which takes a ``Coin`` and returns a new ``Coin`` and a result ``a``. More on the ``->`` syntax for functions later.

``Coin`` and ``play`` are deliberately left obscure in the above. All you have is an action ``getCoin`` to get your hands on a ``Coin`` in a ``Scenario`` context and an action ``flipCoin`` which represents the simplest possible game: A single coin flip resulting in a  ``Face``. You can't play any ``CoinGame`` game on pen and paper as you don't have a coin, but you can write down a script or recipe for a game:

.. literalinclude:: daml/daml-intro-5/Restrictions.daml
  :language: daml
  :start-after: -- COIN_SCENARIO_BEGIN
  :end-before: -- COIN_SCENARIO_END

The ``game`` expression is a ``CoinGame`` in which a coin is flipped three times. If all three tosses return ``Heads``, the result is ``"Win"``, or else ``"Loss"``.

In a ``Scenario`` context you can get a ``Coin``, which is actually a pseudo-random number generator based on LET, and play the game.

*Somehow* the ``Coin`` is threaded through the various actions. If you want to look through the looking glass and understand in-depth what's going on, you can look at the source file to see how the ``CoinGame`` action is implemented, though be warned that the implementation uses a lot of DAML features we haven't introduced yet in this introduction.

More generally, if you want to learn more about Actions (aka Monads), we recommend a general course on functional programming, and Haskell in particular.

Next Up
-------

You can now specify a precise data and data-transformation model for DAML ledgers. In :doc:`6_Parties`, you will learn how to properly involve multiple parties in contracts, how authority works in DAML, and how to build contract models with strong guarantees in contexts with mutually distrusting entities.
