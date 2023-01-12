.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Test Daml Contracts
===================

This chapter is all about testing and debugging the Daml contracts you've built using the tools from earlier chapters. You've already met Daml Script as a way of testing your code inside the IDE. In this chapter you'll learn about more ways to test with Daml Script and its other uses, as well as other tools you can use for testing and debugging. You'll also learn about a few error cases that are most likely to crop up only in actual distributed testing, and which need some care to avoid. Specifically we will cover:

- Daml Test tooling - Script, REPL, and Navigator
- Checking choice coverage
- The ``trace`` and ``debug`` functions
- Contention

Note that this section only covers testing your Daml contracts. For more holistic application testing, please refer to :doc:`/getting-started/testing`.

If you no longer have your projects set up, load all the code for this parts 1 and 2 of this section into two folders ``intro12-part1`` and ``intro12-part2``, by running ``daml new intro12-part1 --template daml-intro-12-part1`` and ``daml new intro12-part2 --template daml-intro-12-part2``.

Daml Test Tooling
-----------------

There are three primary tools available in the SDK to test and interact with Daml contracts. It is highly recommended to explore the respective docs. The :doc:`9_Dependencies` model lends itself well to being tested using these tools.

:doc:`Daml Script </daml-script/index>`

   :doc:`Daml Script </daml-script/index>` should be familiar by now. It's a way to script commands and queries from multiple parties against a Daml Ledger. Unless you've browsed other sections of the documentation already, you have probably used it mostly in the IDE. However, Daml Script can do much more than that. It has four different modes of operation:

   1. Run on a special Script Service in the IDE, providing the Script Views.
   2. Run the Script Service via the CLI, which is useful for quick regression testing.
   3. Start a Sandbox and run against that for regression testing against an actual Ledger API.
   4. Run against any other already running Ledger.

:doc:`Daml Navigator </tools/navigator/index>`

  Daml Navigator is a UI that runs against a Ledger API and allows interaction with contracts.

:doc:`Daml REPL </daml-repl/index>`

  If you want to do things interactively, Daml REPL is the tool to use. The best way to think of Daml REPL is as an interactive version of Daml Script, but it doubles up as a language REPL (Read-Evaluate-Print Loop), allowing you to evaluate pure expressions and inspect the results.

Debug, Trace, and Stacktraces
-----------------------------

The above demonstrates nicely how to test the happy path, but what if a function doesn't behave as you expected? Daml has two functions that allow you to do fine-grained printf debugging: ``debug`` and ``trace``. Both allow you to print something to StdOut if the code is reached. The difference between ``debug`` and ``trace`` is similar to the relationship between ``abort`` and ``error``:

- ``debug : Text -> m ()`` maps a text to an Action that has the side-effect of printing to StdOut.
- ``trace : Text -> a -> a`` prints to StdOut when the expression is evaluated.

.. code-block:: none

  daml> let a : Script () = debug "foo"
  daml> let b : Script () = trace "bar" (debug "baz")
  [Daml.Script:378]: "bar"
  daml> a
  [DA.Internal.Prelude:532]: "foo"
  daml> b
  [DA.Internal.Prelude:532]: "baz"
  daml>

If in doubt, use ``debug``. It's the easier of the two to interpret the results of.

The thing in the square brackets is the last location. It'll tell you the Daml file and line number that triggered the printing, but often no more than that because full stacktraces could violate subtransaction privacy quite easily. If you want to enable stacktraces for some purely functional code in your modules, you can use the machinery in :doc:`/daml/stdlib/DA-Stack` to do so, but we won't cover that any further here.

Diagnose Contention Errors
--------------------------

The above tools and functions allow you to diagnose most problems with Daml code, but they are all synchronous. The sequence of commands is determined by the sequence of inputs. That means one of the main pitfalls of distributed applications doesn't come into play: Contention.

Contention refers to conflicts over access to contracts. Daml guarantees that there can only be one consuming choice exercised per contract so what if two parties simultaneously submit an exercise command on the same contract? Only one can succeed. Contention can also occur due to incomplete or stale knowledge. Maybe a contract was archived a little while ago, but due to latencies, a client hasn't found out yet, or maybe due to the privacy model, they never will. What all these cases have in common is that someone has incomplete knowledge of the state the ledger will be in at the time a transaction will be processed and/or committed.

If we look back at :ref:`execution_model` we'll see there are three places where ledger state is consumed:

1. A command is submitted by some client, probably looking at the state of the ledger to build that command. Maybe the command includes references to ContractIds that the client believes are active.
2. During interpretation, ledger state is used to look up active contracts.
3. During commit, ledger state is again used to look up contracts and validate the transaction by reinterpreting it.

Collisions can occur both between 1 and 2 and between 2 and 3. Only during the commit phase is the complete relevant ledger state at the time of the transaction known, which means the ledger state at commit time is king. As a Daml contract developer, you need to understand the different causes of contention, be able to diagnose the root cause if errors of this type occur, and be able to avoid collisions by designing contracts appropriately.

Common Errors
~~~~~~~~~~~~~

The most common error messages you'll see are listed below. All of them can be due to one of three reasons.

1. Race Conditions - knowledge of a state change is not yet known during command submission
2. Stale References - the state change is known, but contracts have stale references to keys or ContractIds
3. Ignorance - due to privacy or operational semantics, the requester doesn't know the current state

Following the possible error messages, we'll discuss a few possible causes and remedies.

ContractId Not Found During Interpretation
..........................................

.. code-block:: none

  Command interpretation error in LF-Damle: dependency error: couldn't find contract ContractId(004481eb78464f1ed3291b06504d5619db4f110df71cb5764717e1c4d3aa096b9f).

ContractId Not Found During Validation
......................................

.. code-block:: none

  Disputed: dependency error: couldn't find contract ContractId (00c06fa370f8858b20fd100423d928b1d200d8e3c9975600b9c038307ed6e25d6f).

fetchByKey Error During Interpretation
......................................

.. code-block:: none

  Command interpretation error in LF-Damle: dependency error: couldn't find key com.daml.lf.transaction.GlobalKey@11f4913d.

fetchByKey Dispute During Validation
....................................

.. code-block:: none

  Disputed: dependency error: couldn't find key com.daml.lf.transaction.GlobalKey@11f4913d

lookupByKey Dispute During Validation
......................................

.. code-block:: none

  Disputed: recreated and original transaction mismatch VersionedTransaction(...) expected, but VersionedTransaction(...) is recreated.

Avoid Race Conditions and Stale References
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first thing to avoid is write-write or write-read contention on contracts. In other words, one requester submitting a transaction with a consuming exercise on a contract while another requester submits another exercise or fetch on the same contract. This type of contention cannot be eliminated entirely, for there will always be some latency between a client submitting a command to a participant, and other clients learning of the committed transaction.

Here are a few scenarios and measures you can take to reduce this type of collision:

1. Shard data. Imagine you want to store a user directory on the Ledger. At the core, this is of type ``[(Text, Party)]``, where ``Text`` is a display name and `Party` the associated Party. If you store this entire list on a single contract, any two users wanting to update their display name at the same time will cause a collision. If you instead keep each ``(Text, Party)`` on a separate contract, these write operations become independent from each other.

   The Analogy to keep in mind when structuring your data is that a template defines a table, and a contract is a row in that table. Keeping large pieces of data on a contract is like storing big blobs in a database row. If these blobs can change through different actions, you get write conflicts.
2. Use nonconsuming choices if you can. Nonconsuming exercises have the same contention properties as fetches: they don't collide with each other.

   Contract keys can seem like a way out, but they are not. Contract keys are resolved to Contract IDs during the interpretation phase on the participant node. So it reduces latencies slightly by moving resolution from the client layer to the participant layer, but it doesn't remove the issue. Going back to the auction example above, if Alice sent a command ``exerciseByKey @Auction auctionKey Bid with amount = 100``, this would be resolved to an ``exercise cid Bid with amount = 100`` during interpretation, where ``cid`` is the participant's best guess what ContractId the key refers to.
3. Avoid workflows that encourage multiple parties to simultaneously try to exercise a consuming choice on the same contract. For example, imagine an ``Auction`` contract containing a field ``highestBid : (Party, Decimal)``. If Alice tries to bid $100 at the same time that Bob tries to bid $90, it doesn't matter that Alice's bid is higher. The second transaction to be sequenced will be rejected as it has a write collision with the first. It's better to record the bids in separate ``Bid`` contracts, which can be written to independently. Again, think about how you would structure this data in a relational database to avoid data loss due to race conditions.
4. Think carefully about storing ContractIds. Imagine you had created a sharded user directory according to 1. Each user has a ``User`` contract that store their display name and party. Now you write a chat application where each ``Message`` contract refers to the sender by ``ContractId User``. If the user changes their display name, that reference goes stale. You either have to modify all messages that user ever sent, or become unable to use the sender contract in Daml. If you need to be able to make this link inside Daml, Contract Keys help here. If the only place you need to link ``Party`` to ``User`` is the UI, it might be best to not store contract references in Daml at all.

Collisions Due to Ignorance
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :doc:`Daml Ledger Model </concepts/ledger-model/index>` specifies authorization rules, and privacy rules. Ie it specifies what makes a transaction conformant, and who gets to see which parts of a committed transaction. It does *not* specify how a command is translated to a transaction. This may seem strange at first since the commands - create, exercise, exerciseByKey, createAndExercise - correspond so closely to actions in the ledger model. But the subtlety comes in on the read side. What happens when the participant, during interpretation, encounters a ``fetch``, ``fetchByKey``, or ``lookupByKey``?

To illustrate the problem, let's assume there is a template ``T`` with a contract key, and Alice has witnessed two ``Create`` nodes of a contract of type ``T`` with key ``k``, but no corresponding archive nodes. Alice may not be able to order these two nodes causally in the sense of "one create came before the other". See :doc:`/concepts/local-ledger` for an in-depth treatment of causality on Daml Ledgers.

So what should happen now if Alice's participant encounters a ``fetchByKey @T k`` or ``lookupByKey @T k`` during interpretation? What if it encounters a ``fetch`` node? These decisions are part of the operational semantics, and the decision of what should happen is based on the consideration that the chance of a participant submitting an invalid transaction should be minimized.

If a ``fetch`` or ``exercise`` is encountered, the participant resolves the contract as long as it has not witnessed an archive node for that contract - ie as long as it can't guarantee that the contract is no longer active. The rationale behind this is that ``fetch`` and ``exercise`` use ContractIds, which need to come from somewhere: Command arguments, Contract arguments, or key lookups. In all three cases, someone believes the ContractId to be active still so it's worth trying.

If a ``fetchByKey`` or ``lookupByKey`` node is encountered, the contract is only resolved if the requester is a stakeholder on an active contract with the given key. If that's not the case, there is no reason to believe that the key still resolves to some contract that was witnessed earlier. Thus, when using contract keys, make sure you make the likely requesters of transactions observers on your contracts. If you don't, ``fetchByKey`` will always fail, and ``lookupByKey`` will always return ``None``.

Let's illustrate how collisions and operational semantics and interleave:

1. Bob creates ``T`` with key ``k``. Alice is not a stakeholder.
2. Alice submits a command resulting in well-authorized ``lookupByKey @T k`` during interpretation. Even if Alice witnessed 1, this will resolve to a ``None`` as Alice is not a stakeholder. This transaction is invalid at the time of interpretation, but Alice doesn't know that.
3. Bob submits an ``exerciseByKey @T k Archive``.
4. Depending on which of the transactions from 2 and 3 gets sequenced first, either just 3, or both 2 and 3 get committed. If 3 is committed before 2, 2 becomes valid while in transit.

As you can see, the behavior of ``fetch``, ``fetchByKey`` and ``lookupByKey`` at interpretation time depend on what information is available to the requester at that time. That's something to keep in mind when writing Daml contracts, and something to think about when encountering frequent "Disputed" errors.

Checking Coverage
-----------------

When ``daml test`` runs, it analyzes the ledger record to produce a report on what percentage of templates were created and which interface and template choices were exercised during our tests.

Flags Controlling Tests and Coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``daml test`` has four important options which affect the coverage report: ``--test-pattern PATTERN``, ``--files FILE``, ``--show-coverage``, and ``--all``:

Enabling ``--show-coverage`` lists by name any templates, choices, and interfaces which are not covered. By default, the report only reports percentage of coverage.

The remaining three options affect which tests are run. The coverage report is in aggregate for all tests run - tests that don't run do not count towards coverage.

* Passing ``--test-pattern <PATTERN>`` runs only the local tests which match ``PATTERN``.
* Passing ``--files <FILE>`` runs only the tests found in ``FILE``.
* Enabling ``--all`` runs tests in dependency modules as well. Note: it is not affected by ``test-pattern`` - all external tests are run, and ``test-pattern`` only restricts local tests.

Define templates, choices, and interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To demonstrate how the coverage report works, we start by defining three dummy templates, ``T1``, ``T2``, and ``T3``. Each template has two dummy choices:

.. literalinclude:: daml/daml-intro-12-part1/daml/Token_Coverage_Part1.daml
  :language: daml
  :start-after: -- TEMPLATE_DEFINITIONS_START
  :end-before: -- TEMPLATE_DEFINITIONS_END

We also define an interface ``I`` with instances for ``T1`` and ``T2``:

.. literalinclude:: daml/daml-intro-12-part1/daml/Token_Coverage_Part1.daml
  :language: daml
  :start-after: -- INTERFACE_DEFINITIONS_START
  :end-before: -- INTERFACE_DEFINITIONS_END

Start testing
~~~~~~~~~~~~~

By writing a test which selectively creates and exercises only some of these templates and choices, we will see how the coverage report shows us templates and choices we haven't created and exercised respectively.

To start, the test allocates a single party, ``alice``, which we will use for the whole test:

.. literalinclude:: daml/daml-intro-12-part1/daml/Token_Coverage_Part1.daml
  :language: daml
  :start-after: -- ALLOCATE_PARTY_START
  :end-before: -- ALLOCATE_PARTY_END

Template creation coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~

The coverage report mentions which templates were defined but never created. For example, the following test creates contracts out of only ``T1`` and ``T2``, never creating instances of template ``T3``:

.. literalinclude:: daml/daml-intro-12-part1/daml/Token_Coverage_Part1.daml
  :language: daml
  :start-after: -- CREATE_TEMPLATES_START
  :end-before: -- CREATE_TEMPLATES_END

Running ``daml test --show-coverage`` reports how many templates were defined (3), how many were created (2, 66.7%), and the names of those that weren't created (``T3``):

.. code-block::

  > daml test --show-coverage
  ...
  Modules internal to this package:
  - Internal templates
    3 defined
    2 ( 66.7%) created
    internal templates never created: 1
      Token_Coverage_Part1:T3
  ...

Template choice exercise coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The coverage report also tracks which choices were exercised. For example, the following test exercises the first and second choices of ``T1`` and the second choice of ``T2``. It also archives ``T1``, but not ``T2``.

.. literalinclude:: daml/daml-intro-12-part1/daml/Token_Coverage_Part1.daml
  :language: daml
  :start-after: -- EXERCISE_TEMPLATES_START
  :end-before: -- EXERCISE_TEMPLATES_END

``daml test --show-coverage`` reports that the test exercised 4 out of 9 choices, and lists the choices that weren't exercised, including the second choice of ``T2`` and all the choices on ``T3``.

Note that ``Token_Coverage_Part1:T2:Archive`` is included in the list of unexercised choices - because ``t2`` was not archived, its ``Archive`` choice was not run.

.. code-block::

  > daml test --show-coverage
  ...
  - Internal template choices
  9 defined
  4 ( 44.4%) exercised
  internal template choices never exercised: 5
    Token_Coverage_Part1:T2:Archive
    Token_Coverage_Part1:T2:C_T2_2
    Token_Coverage_Part1:T3:Archive
    Token_Coverage_Part1:T3:C_T3_1
    Token_Coverage_Part1:T3:C_T3_2
  ...

Interface choice exercise coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The coverage report also tracks interfaces, with two differences:
* Because interfaces are not created directly but rather cast from templates which implement them, the coverage report cannot not track their creation nor their archival.
* Because interfaces can be cast from many possible implementing templates, the report tracks interface choices by what interface they are exercised on and which template they were cast from. In the report, these interface choices are formatted as ``<module>:<template>:<choice_name>`` - the ``<choice_name>`` tells us the interface, the ``<template>`` tells us the template type an interface contract was cast from.

The following test creates ``t1`` and ``t2`` as before, but casts them immediately to ``I`` to get two contracts of ``I``: ``t1_i`` via ``T1``, and ``t2_i`` via ``T2``. It exercises both choices on the ``t1_i``, but only the first choice on ``t2_i``.

.. literalinclude:: daml/daml-intro-12-part1/daml/Token_Coverage_Part1.daml
  :language: daml
  :start-after: -- EXERCISE_INTERFACES_START
  :end-before: -- EXERCISE_INTERFACES_END

In the coverage report, there are four detected choices, as expected: two choices for the implementation of ``I`` for ``T1``, and two choices for the implementation of ``I`` for ``T2``. Three were exercised, so the only choice that wasn't exercised was ``C_I_1`` for ``T2``, which is reported as ``Token_Coverage_Part1:T2:C_I_1``.

.. code-block::

  > daml test --show-coverage
  ...
  - Internal interface choices
    4 defined
    3 ( 75.0%) exercised
    internal interface choices never exercised: 1
      Token_Coverage_Part1:T2:C_I_2
  ...

Checking Coverage of External Dependencies
------------------------------------------

The coverage report also describes coverage for external templates, interfaces, and choices. In the ``intro12-part1`` directory, run ``daml build --output intro12-part1.dar``, and copy the resulting ``./intro12-part1.dar`` file into the ``intro12-part2`` directory, where the remainder of our commands will be run.

The ``daml.yaml`` configuration file in part2 specifies ``intro12-part1.dar`` as a dependency, letting us import its module.

Definitions
~~~~~~~~~~~

We begin by defining importing the external dependency ``Token_Coverage_Part1`` as ``External`` to bring all of its external templates and interfaces into scope.

.. literalinclude:: daml/daml-intro-12-part2/daml/Token_Coverage_Part2.daml
  :language: daml
  :start-after: -- IMPORT_EXTERNAL_BEGIN
  :end-before: -- IMPORT_EXTERNAL_END

We also define a dummy template ``T`` with no choices, but an implementation of external interface ``External.I``.

.. literalinclude:: daml/daml-intro-12-part2/daml/Token_Coverage_Part2.daml
  :language: daml
  :start-after: -- DEFINE_INTERNAL_TEMPLATE_BEGIN
  :end-before: -- DEFINE_INTERNAL_TEMPLATE_END

Finally, we define an interface ``I`` with one dummy choice, and implementations for our local template ``T`` and the external template ``External.T1``.

.. literalinclude:: daml/daml-intro-12-part2/daml/Token_Coverage_Part2.daml
  :language: daml
  :start-after: -- DEFINE_INTERNAL_INTERFACE_START
  :end-before: -- DEFINE_INTERNAL_INTERFACE_END

Local Definitions
~~~~~~~~~~~~~~~~~

Running ``daml test -p '/^$/'`` to create a coverage report without running any tests - because no tests were run, coverage will be 0% in all cases, but the report will still tally all discovered templates, interfaces, and choices, both external and internal.

.. code-block:: none

  Modules internal to this package:
  - Internal templates
    1 defined
    0 (  0.0%) created
  - Internal template choices
    1 defined
    0 (  0.0%) exercised
  - Internal interface implementations
    3 defined
      2 internal interfaces
      1 external interfaces
  - Internal interface choices
    4 defined
    0 (  0.0%) exercised

  Modules external to this package:
  - External templates
    3 defined
    0 (  0.0%) created in any tests
    0 (  0.0%) created in internal tests
    0 (  0.0%) created in external tests
  - External template choices
    9 defined
    0 (  0.0%) exercised in any tests
    0 (  0.0%) exercised in internal tests
    0 (  0.0%) exercised in external tests
  - External interface implementations
    2 defined
  - External interface choices
    4 defined
    0 (  0.0%) exercised in any tests
    0 (  0.0%) exercised in internal tests
    0 (  0.0%) exercised in external tests

We defined 1 template with 1 default choice (``Archive``), which get reported along with their coverage in the first two sections:

.. code-block:: none

  - Internal templates
    1 defined
    0 (  0.0%) created
  - Internal template choices
    1 defined
    0 (  0.0%) exercised

We also have 3 interface implementations that we have defined locally, ``External.I for T``, ``I for T``, and ``I for External.T1``. Note that while the interface implementations are local, the interfaces that they are defined over can be non-local - in this case we have 2 for the local interface ``I``, and 1 for the external interface ``External.I``. The total number of locally defined implementations, and the breakdown into local interfaces and external interfaces, is presented in the "Internal interface implementations" section.

.. code-block:: none

  - Internal interface implementations
    3 defined
      2 internal interfaces
      1 external interfaces

These local interface implementations provide 4 choices, two from ``External.I for T``, one from ``I for T``, and one from ``I for External.T1``, reported in the next section along with coverage.

.. code-block:: none

  - Internal interface choices
    4 defined
    0 (  0.0%) exercised

External Definitions
~~~~~~~~~~~~~~~~~~~~

By importing ``Token_Coverage_Part1`` as ``External``, we have brought 3 templates, 9 template choices, 2 interface instances, and 4 interface choices into scope from there, which are listed in the external modules section.

.. code-block:: none

  ...
  Modules external to this package:
  - External templates
    3 defined
    ...
  - External template choices
    9 defined
    ...
  - External interface implementations
    2 defined
  - External interface choices
    4 defined
    ...

External, Internal, and "Any" Coverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unlike internal types, externally defined types can be covered by both internal and external tests. As a result, the report for external types distinguishes between coverage provided by internal tests, external tests, and "any" tests (both internal and external tests).

Here we cover how to run internal and external tests simultaneously to get an aggregate report, and how to interpret this report.

The ``--all`` flag runs tests in external modules as well. Run ``daml test --all --test-pattern notests`` in the ``intro12-part2`` directory - this instructs ``daml test`` to run all tests from external modules, and to run local tests matching ``notests``. We have no local tests named ``notests``, so this will only run the ``main`` test from ``part1``. Because the ``main`` test from ``part1`` does not use any of the types defined in ``part2``, the internal section of the resulting coverage report shows 0% everywhere. However, the ``main`` test does exercise many types in ``part1`` which are external to ``part2`` - as a result, the report's "external" section is similar to the "internal" section in the report for ``part1``:

.. code-block:: none

  ...
  Modules external to this package:
  - External templates
    3 defined
    2 ( 66.7%) created in any tests
    0 (  0.0%) created in internal tests
    2 ( 66.7%) created in external tests
  - External template choices
    9 defined
    4 ( 44.4%) exercised in any tests
    0 (  0.0%) exercised in internal tests
    4 ( 44.4%) exercised in external tests
  - External interface implementations
    2 defined
  - External interface choices
    4 defined
    3 ( 75.0%) exercised in any tests
    0 (  0.0%) exercised in internal tests
    3 ( 75.0%) exercised in external tests

Note that unlike the internal section of the report in Part 1, the external section of the report in Part 2 has coverage for internal tests, external tests, and any tests. In this report, we only ran an external test, ``External:main``, so 0 is reported for all internal tests.

Let's write a local test which will create ``External:T3``, a template which the ``External:main`` test does not create.

.. literalinclude:: daml/daml-intro-12-part2/daml/Token_Coverage_Part2.daml
  :language: daml
  :start-after: -- TEST_T3_BEGIN
  :end-before: -- TEST_T3_END

If we run this test on its own using ``daml test --test-pattern testT3``, our external coverage report will show that 1 out of 3 of the external templates in scope were created, 1 by internal tests and 0 by external tests.

.. code-block:: none

  ...
  modules external to this package:
  - external templates
    3 defined
    1 ( 33.3%) created in any tests
    1 ( 33.3%) created in internal tests
    0 (  0.0%) created in external tests
  ...

We can run this test alongside the ``External:main`` test using ``daml test --all --test-pattern testT3``, to get an aggregate coverage report. The report now shows that 2 out of 3 of the external templates in scope were created in ``External:main``, and 1 out of 3 by internal test ``testT3``. Because ``External:main`` creates ``External:T1`` and ``External:T2``, and ``testT3`` creates ``External:T3``, all types are created across our tests, and the overall coverage across any tests is 3 out of 3 (100%).

.. code-block:: none

  ...
  Modules external to this package:
  - External templates
    3 defined
    3 (100.0%) created in any tests
    1 ( 33.3%) created in internal tests
    2 ( 66.7%) created in external tests
  ...

If we define a different local test, ``testT1AndT2``, which creates ``T1`` and ``T2``, running it alongside ``External:Main``, our report shows 2 out of 3 for "internal tests", 2 out of 3 for "external tests", but 2 out of 3 for "any tests"! Because the templates created by each test overlap, ``T3`` is never created and never covered, so despite an abundance of testing for external templates, coverage is still less than 100%.

.. literalinclude:: daml/daml-intro-12-part2/daml/Token_Coverage_Part2.daml
  :language: daml
  :start-after: -- TEST_T1_AND_T2_BEGIN
  :end-before: -- TEST_T1_AND_T2_END

.. code-block:: none

  ...
  Modules external to this package:
  - External templates
    3 defined
    2 ( 66.7%) created in any tests
    2 ( 66.7%) created in internal tests
    2 ( 66.7%) created in external tests
  ...

External template choices and interface instance choices are also reported with "any", internal, and external coverage - we will not cover them here.

Next Up
-------

There's little more to learn about writing Daml at this point that isn't best learned by practice and consulting reference material for both Daml and Haskell. In section 13, :doc:`Interfaces <13_Interfaces>` we will cover the use of interfaces, a feature which aids code reuse across your Daml programs.
