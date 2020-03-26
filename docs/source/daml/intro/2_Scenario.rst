.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

2 Testing templates using scenarios
===================================

In this section you will test the ``Token`` model from :doc:`1_Token` using DAML's inbuilt ``scenario`` language. You'll learn about the basic features of scenarios:

- Getting parties
- Submitting transactions
- Creating contracts
- Testing for failure
- Archiving contracts
- Viewing ledger and final ledger state

.. scenario_basics:

Scenario basics
---------------

A ``Scenario`` is like a recipe for a test, where you can script different parties submitting a series of transactions, to check that your templates behave as you'd expect. You can also script some some external information like party identities, and ledger time.

Below is a basic scenario that creates a ``Token`` for a party called "Alice".

.. literalinclude:: daml/daml-intro-2/Token_Test.daml
  :language: daml
  :start-after: -- TOKEN_TEST_1_BEGIN
  :end-before: -- TOKEN_TEST_1_END

You declare a ``Scenario`` a top-level variable and introduce it using ``scenario do``. ``do`` always starts a block, so the rest of the scenario is indented.

Before you can create any ``Token`` contracts, you need some parties on the test ledger. The above scenario uses the function ``getParty`` to put a party called "Alice" in a variable ``alice``. There are two things of note there:

- Use of ``<-`` instead of ``=``.

  The reason for that is ``getParty`` is an ``Action`` that can only be performed once the ``Scenario`` is run in the context of a ledger. ``<-`` means "run the action and bind the result". It can only be run in that context because, depending on the ledger the scenario is running on, ``getParty`` may have to look up a party identity or create a new party.

  More on ``Actions`` and ``do`` blocks in :doc:`5_Restrictions`.


  If that doesn't quite make sense yet, for the time being you can think of this arrow as extracting the right-hand-side value from the ledger and storing it into the variable on the left.
- The argument ``"Alice"`` to ``getParty`` does not have to be enclosed in brackets. Functions in DAML are called using the syntax ``fn arg1 arg2 arg3``.

With a variable ``alice`` of type ``Party`` in hand, you can submit your first transaction. Unsurprisingly, you do this using the ``submit`` function. ``submit`` takes two arguments: a ``Party`` and an ``Update``.

Just like ``Scenario`` is a recipe for a test, ``Update`` is a recipe for a transaction. ``create Token with owner = alice`` is an ``Update``, which translates to the transaction creating a ``Token`` with owner Alice.

You'll learn all about the syntax ``Token with owner = alice`` in :doc:`3_Data`.

You could write this as ``submit alice (create Token with owner = alice)``, but just like scenarios, you can assemble updates using ``do`` blocks. A ``do`` block always takes the value of the last statement within it so the syntax shown in the scenario above gives the same result, whilst being easier to read.

Running scenarios
-----------------

There are two ways to run scenarios:

- In DAML Studio, providing visualizations of the resulting ledger
- Using the command line, useful for continuous integration

In DAML Studio, you should see the text "Scenario results" just above the line ``token_test_1 = do``. Click on it to display the outcome of the scenario.

.. figure:: images/2_Scenario/scenario_results1.png

This opens the scenario view in a separate column in VS Code. The default view is a tabular representation of the final state of the ledger:

.. figure:: images/2_Scenario/tabular_view1.png

What this display means:

- The big title reading ``Token_Test:Token`` is the identifier of the type of contract that's listed below. ``Token_Test`` is the module name, ``Token`` the template name.

- The first columns, labelled vertically, show which parties know about which contracts. In this simple scenario, the sole party "Alice" knows about the contract she created.

- The second column shows the ID of the contract. This will be explained later.

- The third column shows the status of the contract, either ``active`` or ``archived``.
- The remaining columns show the contract arguments, with one column per field. As expected, field ``owner`` is ``'Alice'``. The single quotation marks indicate that ``Alice`` is a party.

To run the same test from the command line, save your module in a file ``Token_Test.daml`` and run ``daml damlc -- test --files Token_Test.daml``. If your file contains more than one scenario, all of them will be run.

.. _intro_2_failure:

Testing for failure
-------------------

In :doc:`1_Token` you learned that creating a ``Token`` requires the authority of its owner. In other words, it should not be possible for Alice to create a Token for another party and vice versa. A reasonable attempt to test that would be:

.. literalinclude:: daml/daml-intro-2/Token_Test.daml
  :language: daml
  :start-after: -- FAILURE_TEST_1_BEGIN
  :end-before: -- FAILURE_TEST_1_END

However, if you open the scenario view for that scenario, you see the following message:

.. figure:: images/2_Scenario/failure.png

The scenario failed, as expected, but scenarios abort at the first failure. This means that it only tested that Alice can't create a token for Bob, and the second ``submit`` statement was never reached.

To test for failing submits and keep the scenario running thereafter, or fail if the submission succeeds, you can use the ``submitMustFail`` function:

.. literalinclude:: daml/daml-intro-2/Token_Test.daml
  :language: daml
  :start-after: -- TOKEN_TEST_2_BEGIN
  :end-before: -- TOKEN_TEST_2_END

``submitMustFail`` never has an impact on the ledger so the resulting tabular scenario view just shows the two Tokens resulting from the successful ``submit`` statements. Note the new column for Bob as well as the visibilities. Alice and Bob cannot see each others' Tokens.

.. _archiving:

Archiving contracts
-------------------

Archiving contracts works just like creating them, but using ``archive`` instead of ``create``. Where ``create`` takes an instance of a template, ``archive`` takes a reference to a contract.

References to contracts have the type ``ContractId a``, where ``a`` is a *type parameter* representing the type of contract that the ID refers to. For example, a reference to a ``Token`` would be a ``ContractId Token``.

To ``archive`` the Token Alice has created, you need to get a handle on its contract ID. In scenarios, you do this using ``<-`` notation. That's because the contract ID needs to be retrieved from the ledger. How this works is discussed in :doc:`5_Restrictions`.

This scenario first checks that Bob cannot archive Alice's Token and then Alice successfully archives it:

.. literalinclude:: daml/daml-intro-2/Token_Test.daml
  :language: daml
  :start-after: -- TOKEN_TEST_3_BEGIN
  :end-before: -- TOKEN_TEST_3_END

Exploring the ledger
--------------------

The resulting scenario view is empty, because there are no contracts left on the ledger. However, if you want to see the history of the ledger, e.g. to see how you got to that state, tick the "Show archived" box at the top of the ledger view:

.. figure:: images/2_Scenario/archived.png

You can see that there was a ``Token`` contract, which is now archived, indicated both by the "archived" value in the ``status`` column as well as by a strikethrough.

Click on the adjacent "Show transaction view" button to see the entire transaction graph:

.. figure:: images/2_Scenario/tx_graph.png

In the DAML Studio scenario runner, committed transactions are numbered sequentially. The lines starting with ``TX`` indicate that there are three committed transactions, with ids ``#0``, ``#1``, and ``#2``. These correspond to the three ``submit`` and ``submitMustFail`` statements in the scenario.

Transaction ``#0`` has one *sub-transaction* ``#0:0``, which the arrow indicates is a ``create`` of a ``Token``. Identifiers ``#X:Y`` mean ``commit X, sub-transaction Y``. All transactions have this format in the scenario runner. However, this format is a testing feature. In general, you should consider Transaction and Contract IDs to be opaque.

The lines above and below ``create Token_Test:Token`` give additional information:

- ``consumed by: #2:0`` tells you that the contract is archived in sub-transaction ``0`` of commit ``2``.
- ``referenced by #2:0`` tells you that the contract was used in other transactions, and lists their IDs.
- ``known to (since): 'Alice' (#0)`` tells you who knows about the contract. The fact that ``'Alice'`` appears in the list is equivalent to a ``x`` in the tabular view. The ``(#0)`` gives you the additional information that ``Alice`` learned about the contract in commit ``#0``.
- Everything following ``with`` shows the create arguments.

Exercises
---------

To get a better understanding of scenarios, try the following exercises:

1. Write a template for a second type of Token.
2. Write a scenario with two parties and two types of tokens, creating one token of each type for each party and archiving one token for each party, leaving one token of each type in the final ledger view.
3. In :ref:`archiving` you tested that Bob cannot archive Alice's token. Can you guess why the submit fails? How can you find out why the submit fails?

  .. hint::

    Remember that in :ref:`intro_2_failure` we saw a proper error message for a failing submit.

Next up
----------

In :doc:`3_Data` you will learn about DAML's type system, and how you can think of templates as tables and contracts as database rows.
