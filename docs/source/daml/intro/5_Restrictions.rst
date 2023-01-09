.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Add Constraints to a Contract
=============================

You will often want to constrain the data stored or the allowed data transformations in your contract models. In this section, you will learn about the two main mechanisms provided in Daml:

- The ``ensure`` keyword.
- The ``assert``, ``abort`` and ``error`` keywords.

To make sense of the latter, you'll also learn more about the ``Update`` and ``Script`` types and ``do`` blocks, which will be good preparation for :doc:`7_Composing`, where you will use ``do`` blocks to compose choices into complex transactions.

Lastly, you will learn about time on the ledger and in Daml Script.

.. hint::

  Remember that you can load all the code for this section into a folder called ``intro5`` by running ``daml new intro5 --template daml-intro-5``

Template Preconditions
----------------------

The first kind of restriction you may want to put on the contract model are called *template pre-conditions*. These are simply restrictions on the data that can be stored on a contract from that template.

Suppose, for example, that the ``SimpleIou`` contract from :ref:`simple_iou` should only be able to store positive amounts. You can enforce this using the ``ensure`` keyword:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTED_IOU_BEGIN
  :end-before: &&

The ``ensure`` keyword takes a single expression of type ``Bool``. If you want to add more restrictions, use logical operators ``&&``, ``||`` and ``not`` to build up expressions. The below shows the additional restriction that currencies are three capital letters:


.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: ensure
  :end-before: -- RESTRICTED_IOU_END

.. hint::

  The ``T`` here stands for the ``DA.Text`` standard library which has been imported using ``import DA.Text as T``:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTIONS_TEST_BEGIN
  :end-before: -- RESTRICTIONS_TEST_END


Assertions
----------

A second common kind of restriction is one on data transformations.

For example, the simple Iou in :ref:`simple_iou` allowed the no-op where the ``owner`` transfers to themselves. You can prevent that using an ``assert`` statement, which you have already encountered in the context of scripts.

``assert`` does not return an informative error so often it's better to use the function ``assertMsg``, which takes a custom error message:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_CHOICE_BEGIN
  :end-before: -- TRANSFER_CHOICE_END

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_TEST_BEGIN
  :end-before: -- TRANSFER_TEST_END

Similarly, you can write a ``Redeem`` choice, which allows the ``owner`` to redeem an ``Iou`` during business hours on weekdays. The choice doesn't do anything other than archiving the ``SimpleIou``. (This assumes that actual cash changes hands off-ledger:)

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_CHOICE_BEGIN
  :end-before: -- REDEEM_CHOICE_END

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_TEST_BEGIN
  :end-before: -- REDEEM_TEST_END

There are quite a few new time-related functions from the ``DA.Time`` and ``DA.Date`` libraries here. Their names should be reasonably descriptive so how they work won't be covered here, but given that Daml assumes it is run in a distributed setting, we will still discuss time in Daml.

There's also quite a lot going on inside the ``do`` block of the ``Redeem`` choice, with several uses of the ``<-`` operator. ``do`` blocks and ``<-`` deserve a proper explanation at this point.

Time on Daml Ledgers
--------------------

Each transaction on a Daml ledger has two timestamps called the *ledger time (LT)* and the *record time (RT)*. The ledger time is set by the participant, the record time is set by the ledger.

Each Daml ledger has a policy on the allowed difference between LT and RT called the *skew*. The participant has to take a good guess at what the record time will be. If it's too far off, the transaction will be rejected.

``getTime`` is an action that gets the LT from the ledger. In the above example, that time is taken apart into day of week and hour of day using standard library functions from ``DA.Date`` and ``DA.Time``. The hour of the day is checked to be in the range from 8 to 18.

Consider the following example: Suppose that the ledger had a skew of 10 seconds. At 17:59:55, Alice submits a transaction to redeem an Iou. One second later, the transaction is assigned a LT of 17:59:56, but then takes 10 seconds to commit and is recorded on the ledger at 18:00:06. Even though it was committed after business hours, it would be a valid transaction and be committed successfully as ``getTime`` will return 17:59:56 so ``hrs == 17``. Since the RT is 18:00:06, ``LT - RT <= 10 seconds`` and the transaction won't be rejected.

Time therefore has to be considered slightly fuzzy in Daml, with the fuzziness depending on the skew parameter.

For details, see :ref:`Background concepts - time <time>`.

Time in Test Scripts
~~~~~~~~~~~~~~~~~~~~

For tests, you can set time using the following functions:

- ``setTime``, which sets the ledger time to the given time.
- ``passTime``, which takes a ``RelTime`` (a relative time) and moves the ledger by that much.

Time on Ledgers
~~~~~~~~~~~~~~~~~

On a distributed Daml ledger, there are no guarantees that ledger time or record time are strictly increasing. The only guarantee is that ledger time is increasing with causality. That is, if a transaction ``TX2`` depends on a transaction ``TX1``, then the ledger enforces that the LT of ``TX2`` is greater than or equal to that of ``TX1``:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- CAUSALITY_TEST_BEGIN
  :end-before: -- CAUSALITY_TEST_END

Actions and ``do`` Blocks
-------------------------

You have come across ``do`` blocks and ``<-`` notations in two contexts by now: ``Script`` and ``Update``. Both of these are examples of an ``Action``, also called a *Monad* in functional programming. You can construct ``Actions`` conveniently using ``do`` notation.

Understanding ``Actions`` and ``do`` blocks is therefore crucial to being able to construct correct contract models and test them, so this section will explain them in some detail.

Pure Expressions Compared to Actions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Expressions in Daml are pure in the sense that they have no side-effects: they neither read nor modify any external state. If you know the value of all variables in scope and write an expression, you can work out the value of that expression on pen and paper.

However, the expressions you've seen that used the ``<-`` notation are not like that. For example, take ``getTime``, which is an ``Action``. Here's the example we used earlier:

.. code-block:: daml

   now <- getTime

You cannot work out the value of ``now`` based on any variable in scope. To put it another way, there is no expression ``expr`` that you could put on the right hand side of ``now = expr``. To get the ledger time, you must be in the context of a submitted transaction, and then look at that context.

Similarly, you've come across ``fetch``. If you have ``cid : ContractId Account`` in scope and you come across the expression ``fetch cid``, you can't evaluate that to an ``Account`` so you can't write ``account = fetch cid``. To do so, you'd have to have a ledger you can look that contract ID up on.

Actions and Impurity
~~~~~~~~~~~~~~~~~~~~~

Actions are a way to handle such "impure" expressions. ``Action a`` is a type class with a single parameter ``a``, and ``Update`` and ``Script`` are instances of ``Action``. A value of such a type ``m a`` where ``m`` is an instance of ``Action`` can be interpreted as "a recipe for an action of type ``m``, which, when executed, returns a value ``a``".

You can always write a recipe using just pen and paper, but you can't cook it up unless you are in the context of a kitchen with the right ingredients and utensils. When cooking the recipe you have an effect -- you change the state of the kitchen -- and a return value -- the thing you leave the kitchen with.

- An ``Update a`` is "a recipe to update a Daml ledger, which, when committed, has the effect of changing the ledger, and returns a value of type ``a``". An update to a Daml ledger is a transaction so equivalently, an ``Update a`` is "a recipe to construct a transaction, which, when executed in the context of a ledger, returns a value of type ``a``".
- A ``Script a`` is "a recipe for a test, which, when performed against a ledger, has the effect of changing the ledger in ways analogous to those available via the API, and returns a value of type ``a``".

Expressions like ``getTime``, ``allocateParty party``, ``passTime time``, ``submit party commands``, ``create contract`` and ``exercise choice`` should make more sense in that light. For example:

- ``getTime : Update Time`` is the recipe for an empty transaction that also happens to return a value of type ``Time``.
- ``passTime (days 10) : Script ()`` is a recipe for a transaction that doesn't submit any transactions, but has the side-effect of changing the LT of the test ledger. It returns ``()``, also called ``Unit`` and can be thought of as a zero-tuple.
- ``create iou : Update (ContractId Iou)``, where ``iou : Iou`` is a recipe for a transaction consisting of a single ``create`` action, and returns the contract id of the created contract if successful.
- ``submit alice (createCmd iou) : Script (ContractId Iou)`` is a recipe for a script in which Alice sends the command ``createCmd iou`` to the ledger which produces a transaction and a return value of type ``ContractId Iou`` and returns that back to Alice.

``Commands`` is a bit more restricted than
``Script`` and ``Update`` as it represents a list of independent
commands sent to the ledger. You can still use ``do`` blocks but if
you have more than one command in a single ``do`` block you need to
enable the ``ApplicativeDo`` extension at the beginning of your file.
In addition to that, the last statement in such a ``do`` block must be of the form
``return expr`` or ``pure expr``.
``Applicative`` is
a more restricted version of ``Action`` that enforces that there are
no dependencies between commands. If you do have dependencies between
commands, you can always wrap it in a choice in a helper template and
call that via ``createAndExerciseCmd`` just like we did to call
``fetchByKey``. Alternatively, if you do not need them to be part of the
same transaction, you can make multiple calls to ``submit``:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- BEGIN_EXT
  :end-before: -- END_EXT

Chain Actions With ``do`` Blocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An action followed by another action, possibly depending on the result of the first action, is just another action. Specifically:

- A transaction is a list of actions. So a transaction followed by another transaction is again a transaction.
- A script is a list of interactions with the ledger (``submit``, ``allocateParty``, ``passTime``, etc). So a script followed by another script is again a script.

This is where ``do`` blocks come in. ``do`` blocks allow you to build complex actions from simple ones, using the results of earlier actions in later ones:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- DO_DEMO_BEGIN
  :end-before: -- DO_DEMO_END

Above, we see ``do`` blocks in action for both ``Script`` and ``Update``.

Wrap Values in Actions
~~~~~~~~~~~~~~~~~~~~~~

You may already have noticed the use of ``return`` in the redeem choice. ``return x`` is a no-op action which returns value ``x`` so ``return 42 : Update Int``. Since ``do`` blocks always return the value of their last action, ``sub_script2 : Script Int``.

Failing Actions
---------------

Not only are ``Update`` and ``Script`` examples of ``Action``, they are both examples of actions that can fail, e.g. because a transaction is illegal or the party retrieved via ``allocateParty`` doesn't exist on the ledger.

Each has a special action ``abort txt`` that represents failure, and that takes on type ``Update ()`` or ``Script ()`` depending on context .

Transactions succeed or fail *atomically* as a whole. Scripts on the other hand do not fail atomically: while each ``submit`` is atomic, if a ``submit`` succeeded and the script fails later, the effects of that ``submit`` will still be applied to the ledger.

The last expression in the ``do`` block of the ``Redeem`` choice is a pattern matching expression on ``dow``. It has type ``Update ()`` and is either an ``abort`` or ``return`` depending on the day of week. So during the week, it's a no-op and on weekends, it's the special failure action. Thanks to the atomicity of transactions, no transaction can ever make use of the ``Redeem`` choice on weekends, because it fails the entire transaction.

A Sample Action
---------------

If the above didn't make complete sense, here's another example to explain what actions are more generally, by creating a new type that is also an action. ``CoinGame a`` is an ``Action a`` in which a ``Coin`` is flipped. The ``Coin`` is a pseudo-random number generator and each flip has the effect of changing the random number generator's state. Based on the ``Heads`` and ``Tails`` results, a return value of type ``a`` is calculated:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- COIN_BEGIN
  :end-before: -- COIN_END

A ``CoinGame a`` exposes a function ``play`` which takes a ``Coin`` and returns a new ``Coin`` and a result ``a``. More on the ``->`` syntax for functions later.

``Coin`` and ``play`` are deliberately left obscure in the above. All you have is an action ``getCoin`` to get your hands on a ``Coin`` in a ``Script`` context and an action ``flipCoin`` which represents the simplest possible game: a single coin flip resulting in a  ``Face``.

You can't play any ``CoinGame`` game on pen and paper as you don't have a coin, but you can write down a script or recipe for a game:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- COIN_SCENARIO_BEGIN
  :end-before: -- COIN_SCENARIO_END

The ``game`` expression is a ``CoinGame`` in which a coin is flipped three times. If all three tosses return ``Heads``, the result is ``"Win"``, or else ``"Loss"``.

In a ``Script`` context you can get a ``Coin`` using the ``getCoin`` action, which uses the LT to calculate a seed, and play the game.

*Somehow* the ``Coin`` is threaded through the various actions. If you want to look through the looking glass and understand in-depth what's going on, you can look at the source file to see how the ``CoinGame`` action is implemented, though be warned that the implementation uses a lot of Daml features we haven't introduced yet in this introduction.

More generally, if you want to learn more about Actions (aka Monads), we recommend a general course on functional programming, and Haskell in particular. See :ref:`haskell-connection` for some suggestions.

Errors
------

Above, you've learnt about ``assertMsg`` and ``abort``, which represent (potentially) failing actions. Actions only have an effect when they are performed, so the following script succeeds or fails depending on the value of ``abortScript``:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- NON_PERFORMED_ABORT_BEGIN
  :end-before: -- NON_PERFORMED_ABORT_END

However, what about errors in contexts other than actions? Suppose we wanted to implement a function ``pow`` that takes an integer to the power of another positive integer. How do we handle that the second parameter has to be positive?

One option is to make the function explicitly partial by returning an ``Optional``:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- OPTIONAL_POW_BEGIN
  :end-before: -- OPTIONAL_POW_END

This is a useful pattern if we need to be able to handle the error case, but it also forces us to always handle it as we need to extract the result from an ``Optional``. We can see the impact on convenience in the definition of the above function.  In cases, like division by zero or the above function, it can therefore be preferable to fail catastrophically instead:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- ERROR_POW_BEGIN
  :end-before: -- ERROR_POW_END

The big downside to this is that even unused errors cause failures. The following script will fail, because ``failingComputation`` is evaluated:

.. literalinclude:: daml/daml-intro-5/daml/Restrictions.daml
  :language: daml
  :start-after: -- NON_PERFORMED_ERROR_BEGIN
  :end-before: -- NON_PERFORMED_ERROR_END

``error`` should therefore only be used in cases where the error case is unlikely to be encountered, and where explicit partiality would unduly impact usability of the function.

Next Up
-------

You can now specify a precise data and data-transformation model for Daml ledgers. In :doc:`6_Parties`, you will learn how to properly involve multiple parties in contracts, how authority works in Daml, and how to build contract models with strong guarantees in contexts with mutually distrusting entities.
