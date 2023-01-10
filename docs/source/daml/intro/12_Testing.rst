.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Test Daml Contracts
===================

This chapter is all about testing and debugging the Daml contracts you've built using the tools from earlier chapters. You've already met Daml Script as a way of testing your code inside the IDE. In this chapter you'll learn about more ways to test with Daml Script and its other uses, as well as other tools you can use for testing and debugging. You'll also learn about a few error cases that are most likely to crop up only in actual distributed testing, and which need some care to avoid. Specifically we will cover:

- Daml Test tooling - Script, REPL, and Navigator
- The ``trace`` and ``debug`` functions
- Contention

Note that this section only covers testing your Daml contracts. For more holistic application testing, please refer to :doc:`/getting-started/testing`.

If you no longer have your projects set up, please follow the setup instructions in :doc:`9_Dependencies` to get hold of the code for this chapter. There is no code specific to this chapter.

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

Next Up
-------

You've reached the end of the Introduction to Daml. Congratulations! You can move on to the :doc:`nexs steps <99_NextSteps>` to understand how to put in practice the skills you've learned. Happy Daml'ing!
