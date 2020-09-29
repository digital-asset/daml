.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

11 Testing DAML Contracts
=========================

This chapter is all about testing and debugging the DAML contracts you've build using the tools from chapters 1-10. You've already met DAML Script as a way of testing your code inside the IDE. In this chapter you'll learn about other uses of DAML Script, as well as other tools you can use for testing and debugging. You'll also learn about a few error cases that are most likely to crop up only in actual distributed testing, and which need some care to avoid. Specifically we will cover:

- DAML Test tooling - Script, REPL, and Navigator
- The ``trace`` and ``debug`` functions
- Contention

Note that this section only covers testing your DAML contracts. For more holistic application testing, please refer to :doc:`/getting-started/testing`.

If you no longer have your projects set up, please follow the setup instructions in :doc:`8_Upgrading` to get hold of the code for this chapter. There is no code specific to this chapter.

DAML Test Tooling
-----------------

There are three primary tools available in the SDK to test and interact with DAML contracts:

- DAML Script
- DAML Navigator
- DAML REPL

DAML Script
~~~~~~~~~~~

:doc:`DAML Script </daml-script/index>` should be familiar by now. It's a way to script commands and queries from multiple parties against a DAML Ledger. Unless you've browsed other sections of the documentation already, you have probably used it mostly in the IDE. However, DAML Script can do much more than that. It has four different modes of operation:

1. Run on a special Script Service in the IDE, providing the Script Views.
2. Run the Script Service via the CLI, which is useful for quick regression testing.
3. Start a Sandbox and run against that for regression testing against an actual Ledger API.
4. Run against any other already running Ledger.

Let's quickly see 2-4 in action. 

Script Service CLI Testing
..........................

In your V2 Asset project, run ``daml test``. This runs the same Scripts that are run in the IDE on the same in-memory Ledger called the Script Service. Instead of views of the resulting ledger, it outputs successes and failures.

.. code-block:: shell

  daml/Test/Intro/Asset/V2/Role.daml:setupRoles: ok, 2 active contracts, 4 transactions.
  daml/Test/Intro/Asset/V2/Role.daml:test_issuance: ok, 3 active contracts, 5 transactions.
  daml/Test/Intro/Asset/V2.daml:test_split: ok, 4 active contracts, 6 transactions.
  daml/Test/Intro/Asset/V2.daml:test_merge: ok, 3 active contracts, 7 transactions.
  daml/Test/Intro/Asset/V2.daml:test_asset: ok, 3 active contracts, 7 transactions.
  daml/Test/Intro/Asset/V2/Trade.daml:tradeSetup: ok, 6 active contracts, 10 transactions.
  daml/Test/Intro/Asset/V2/Trade.daml:test_trade: ok, 6 active contracts, 15 transactions.

If you add the command line flag ``--junit``, ``daml test`` can write the results in jUnit format, which is useful for automated testing.

Sandbox CLI Testing
...................

The next step up in terms of how "real" the tests are is the command ``daml test-script``. Unlike ``daml test``, these tests are performed via a Ledger API against a real Ledger. If no ledger host and port are provided, a temporary Sandbox is started to run the tests against.

In the same project, make sure you have compiled the contracts (``daml build -o assets.dar``), then run ``daml test-script --dar assets.dar``. You'll see a lot more output this time, starting with a Sandbox being started up, followed by DAML Scrpipt outputs. Each script should finish with a message of the type ``Test.Intro.Asset.V2:test_split SUCCESS``.

Script Against a Running Ledger
...............................

The last thing we can do with DAML Script is to run against a live ledger through the Ledger API. To see this in action, we'll initialize a Sandbox Ledger with the V1 Asset model. Go to your Chapter 8 Upgrade project.

#. Start a sandbox with ``daml sandbox``.
#. In a second terminal window, rebuild if needed with ``daml build -o upgrade.dar``.
#. Upload the DAR file form the chapter 8 Upgrade project by running ``daml ledger upload-dar upgrade.dar``.
#. Run the v1Setup Script with ``daml script --dar upgrade.dar --ledger-host localhost --ledger-port 6865 --script-name Test.Intro.Asset.Upgrade.V1Setup:v1Setup``

By default, this doesn't log much so if it returns with code 0, you can assume it was successful. But how can we check?

DAML Navigator
~~~~~~~~~~~~~~

:doc:`/tools/navigator/index` is a UI that runs against a Ledger API and allows interaction with contracts. The easiest way to start it against an already running ledger is using ``daml ledger navigator``. If successful, it will tell you ``Frontend running at http://localhost:4000.``. Navigate to that URL and log in as Alice. You'll see the three contracts created by the setup script: One ``AssetHolder`` contract for each bank, and one ``Asset``. If so the script was successful.

We now want to run an upgrade. We could do that using Navigator, but it would be some work. Try it out by logging in as an issuer, issuing and UpgradeInvite, and then try to follow the upgrade process by hand. It's quite cumbersome and we already have script to do it, so let's use those.

DAML Script with Inputs
~~~~~~~~~~~~~~~~~~~~~~~

You may be tempted to run the script ``Test.Intro.Asset.Upgrade.V2.testUpgrade``, but beware! That script calls ``v1Setup``, which allocates parties. If we ran that script, we would run an entirely new setup, and then the upgrade on the contracts resulting from the second upgrade. Try it out if you want to see that in action.

Fortunately, DAML Script can take inputs in JSON format, and all the upgrade scripts are structures with a ``Relationship`` value as input. In order to use that feature, we first have to get our hands on some Party IDs. Run ``daml ledger list-parties`` to see all allocated parties. You'll get something like

.. code-block:: shell

  Listing parties at localhost:6865
  PartyDetails {party = 'party-7c7129dd', displayName = "Alice", isLocal = True}
  PartyDetails {party = 'party-09d3b36d', displayName = "Bob", isLocal = True}
  PartyDetails {party = 'party-5820624d', displayName = "USD_Bank", isLocal = True}
  PartyDetails {party = 'party-12e3ae8a', displayName = "EUR_Bank", isLocal = True}

Let's run the upgrade for Alice and USD_Bank. The JSON input for a ``Relationship`` looks as you might expect:

.. code-block:: json

  {
    "issuer" : "party-5820624d",
    "owner" : "party-7c7129dd"
  }

To run the complete upgrade using DAML Script, run the below command, replacing the party IDs.

.. code-block:: shell

  daml script --dar upgrade.dar --ledger-host localhost --ledger-port 6865 --script-name Test.Intro.Asset.Upgrade.V2:runCompleteUpgrade --input-file <(echo '{"issuer" : "party-5820624d", "owner" : "party-7c7129dd"}')

Note that the ``--input-file`` flag expects a file. The ``<(..)`` is a bit of unix shell magic to provide the file contents inline. If it doesn't work on your shell, put the JSON into a file ``relationship.json``, and supply that instead. For more information on this, please refer to the :doc:`DAML Script </daml-script/index>` and :doc:`JSON Format </json-api/lf-value-specification>` docs.

If you still have the Navigator open, you'll see two of Alice's three contracts upgrade as you run the script. That worked as a way to test an upgrade, but it would be cumbersome if we wanted to step through the individual steps of that script one by one.

DAML REPL
~~~~~~~~~

If you want to do things interactively, :doc:`DAML REPL </daml-repl/index>` is the tool to use. The best way to think of DAML REPL is as an interactive version of DAML Script. Run it using

.. code-block:: shell

  daml repl --ledger-host localhost --ledger-port 6865 upgrade.dar -i 8Upgrade

DAML REPL acts both as a language REPL (Read-Evaluate-Print Loop) as well as a shell to interact with a ledger. In other words, we can test pure expressions as well as sending commands and querying. As an example, you can use the ``length`` function from Prelude:

.. code-block:: shell

  daml> length [1,3,2]
  3

You can use ``:help`` to show the available meta-commands. Running ``:show imports`` will show you that the modules form the upgrade project are already imported. That's the effect of the ``-i 8Upgrade`` in the command above. You an load and unload other modules using the ``:module`` command:

.. code-block:: shell

  daml> :module + DA.List
  daml> sort [1,3,2]
  [1,2,3]

Now let's run an upgrade step by step. We first need to get our hands on the Alice and EUR_Bank parties. For that, we use the ``listKnownParties`` action and filter according to display names:

.. code-block:: shell

  daml> parties <- listKnownParties
  daml> parties
  [PartyDetails {party = 'party-3cca7cc5', displayName = Some "Alice", isLocal = True},PartyDetails {party = 'party-4b70184e', displayName = Some "Bob", isLocal = True},PartyDetails {party = 'party-6e5b60bf', displayName = Some "USD_Bank", isLocal = True},PartyDetails {party = 'party-3d836540', displayName = Some "EUR_Bank", isLocal = True}]
  daml> let alice = (parties!!0).party
  daml> let eurbank = (parties!!3).party

Now we can run upgrade steps just like we would in Script:

.. code-block:: shell

  daml> let rel = Relationship with issuer=eurbank; owner=alice
  daml> initiateUpgrade rel
  daml> acceptUpgrade rel

All of the script return ``()`` so the REPL doesn't show us anything here. To make sure something is happening, let's query the ledger at this point.


.. code-block:: shell

  daml> query @Upgrade alice
  [(<contract-id>,Upgrade {issuer = 'party-3d836540', owner = 'party-3cca7cc5'})]

There's an ``Upgrade`` contract as expected. Now Alice didn't actually have any EUR holdings so we should be able to skip the ``performUpgrade`` step. Let's check whether the upgrade is complete according to the check functions:

.. code-block:: shell

  daml> ownerCheckUpgradeComplete rel
  True
  daml> issuerCheckUpgradeComplete rel
  True

As expected, there's nothing to upgrade so let's interactively complete this upgrade.

.. code-block:: shell

  daml> confirmCompletion rel
  daml> completeUpgrade rel

  As you can see, DAML Repl is able to mix and match scripts and custom interactions seamlessly. 

Debug, Trace, and Stacktraces
-----------------------------

The above demonstrates nicely how to test the happy path, but what if a function doesn't behave as you expected? DAML has two functions that allow you to do fine-grained printf debugging: ``debug`` and ``trace``. Both allow you to print something to StdOut if the code is reached. The difference between ``debug`` and ``trace`` is similar to the relationship between ``abort`` and ``error``:

- ``debug : Text -> m ()`` maps a text to an Action that has the side-effect of printing to StdOut.
- ``trace : Text -> a -> a`` prints to StdOut when the expression is evaluated. 

.. code-block:: shell

  daml> let a : Script () = debug "foo"
  daml> let b : Script () = trace "bar" (debug "baz")
  [Daml.Script:378]: "bar"
  daml> a
  [DA.Internal.Prelude:540]: "foo"
  daml> b
  [DA.Internal.Prelude:540]: "baz"
  daml>

If in doubt, use ``debug``. It's the easier of the two to interpret the results of.

The thing in the square brackets is the last location. It'll tell you the DAML file and line number that triggered the printing, but often no more than that because full stacktraces could violate subtransaction privacy quite easily. If you want to enable stacktraces for some purely functional code in your modules, you can use the machinery in :doc:`/daml/stdlib/DA-Stack` to do so, but we won't cover that any further here.

Diagnosing Contention Errors
----------------------------

The above tools and functions allow you to diagnose most problems with DAML code, but they are all synchronous. The sequence of commands is determined by the sequence of inputs. That means one of the main pitfalls of distributed applications doesn't come into play: Contention.

Contention refers to conflicts over access to contracts. DAML guarantees that there can only be one consuming choice exercised per contract so what if two parties simultaneously submit an exercise command on the same contract? Only one can succeed. Contention can also occur due to incomplete or stale knowledge. Maybe a contract was archived a little while ago, but due to latencies, a client hasn't found out yet, or maybe due to the privacy model, they never will. What all these cases have in common is that someone has incomplete knowledge of the state the ledger will be in at the time a transaction will be processed and/or committed.

Look back that the :ref:`execution_model`. There are three places where ledger state is consumed:

1. A command is submitted by some client, probably looking at the state of the ledger to build that command. Maybe the command includes references to ContractIds that the client believes active.
2. During interpretation, ledger state is used to to look up active contracts.
3. During commit, ledger state is again used to look up contracts and validate the transaction by reinterpreting it.

Collisions can occur both between 1 and 2 and between 2 and 3. Only during the commit phase is the complete relevant ledger state at the time of the transaction known, which means the ledger state at commit time is king. As a DAML contract developer, you need to understand the different causes of contention, be able to diagnose the root cause if errors of this type occur, and be able to avoid collisions by designing contracts appropriately. 

Common Errors
~~~~~~~~~~~~~

The most common error messages you'll see are listed below. All of them can be due to one of three reasons.

1. Race Conditions - knowledge of a state change is not yet known during command submission
2. Stale References - the state change is known, but contracts have stale references to keys or ContractIds
3. Ignorance - due to privacy or operational semantics, the requester doesn't know the current state

Following the possible error messages, we'll discuss a few possible causes and remedies.

ContractId Not Found During Interpretation
..........................................

.. code-block:: shell 

  Command interpretation error in LF-DAMLe: dependency error: couldn't find contract ContractId(004481eb78464f1ed3291b06504d5619db4f110df71cb5764717e1c4d3aa096b9f).

ContractId Not Found During Validation
......................................

.. code-block:: shell

  Disputed: dependency error: couldn't find contract ContractId (00c06fa370f8858b20fd100423d928b1d200d8e3c9975600b9c038307ed6e25d6f).

fetchByKey Error during Interpretation
......................................

.. code-block:: shell

  Command interpretation error in LF-DAMLe: dependency error: couldn't find key com.daml.lf.transaction.GlobalKey@11f4913d.

fetchByKey Dispute During Validation
....................................

.. code-block:: shell

  Disputed: dependency error: couldn't find key com.daml.lf.transaction.GlobalKey@11f4913d

lookupByKey Distpute During Validation
......................................

.. code-block:: shell

  Disputed: recreated and original transaction mismatch VersionedTransaction(...) expected, but VersionedTransaction(...) is recreated.

Avoiding Race Conditions and Stale References
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first thing to avoid is write-write or write-read contention on contracts. In other words, one requester submitting a transaction with a consuming exercise on a contract while another requester submits another exercise or fetch on the same contract. This type of contention cannot be eliminated entirely, for there will always be some latency between a client submitting a command to a participant, and other clients learning of the committed transaction.

Here are a few scenarios and measures you can take to reduce this type of collision:

1. Shard data. Imagine you want to store a user directory on the Ledger. At the core, this is of type ``[(Text, Party)]``, where ``Text`` is a display name and `Party` the associated Party. If you store this entire list on a single contract, any two users wanting to update their display name at the same time will cause a collision. If you instead keep each ``(Text, Party)`` on a separate contract, these write operations become independent from each other.
   
   The Analogy to keep in mind when structuring your data is that a template defines a table, and a contract is a row in that table. Keeping large pieces of data on a contract is like storing big blobs in a database row. If these blobs can change through different actions, you get write conflicts.
2. Use nonconsuming choices if you can. Nonconsuming exercises have the same contention properties as fetches: they don't collide with each other.
   
   Contract keys can seem like a way out, but they are not. Contract keys are resolved to Contract IDs during the interpretation phase on the participant node. So it reduces latencies slightly by moving resolution from the client layer to the participant layer, but it doesn't remove the issue. Going back to the auction example above, if Alice sent a command ``exerciseByKey @Auction auctionKey Bid with amount = 100``, this would be resolved to an ``exercise cid Bid with amount = 100`` during interpretation, where ``cid`` is the participant's best guess what ContractId the key refers to.
3. Avoid workflows that encourage multiple parties to simultaneously try to exercise a consuming choice on the same contract. For example, imagine an ``Auction`` contract containing a field ``highestBid : (Party, Decimal)``. If Alice tries to bid $100 at the same time that Bob tries to bid $90, it doesn't matter that Alice's bid is higher. The second transaction to be sequenced will be rejected as it has a write collision with the first. It's better to record the bids in separate ``Bid`` contracts, which can be written to independently. Again, think about how you would structure this data in a relational database to avoid data loss due to race conditions.
4. Think carefully about storing ContractIds. Imagine you had created a sharded user directory according to 1. Each user has a ``User`` contract that store their display name and party. Now you write a chat application where each ``Message`` contract refers to the sender by ``ContractId User``. If the user changes their display name, that reference goes stale. You either have to modify all messages that user ever sent, or become unable to use the sender contract in DAML.

Collisions due to Ignorance
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :doc:`DAML Ledger Model </concepts/ledger-model/index>` specifies authorization rules, and privacy rules. Ie it specifies what makes a transaction conformant, and who gets to see which parts of a committed transaction. It does *not* specify how a command is translated to a transaction. This may seem strange at first since the commands - create, exercise, exerciseByKey, createAndExercise - correspond so closely to actions in the ledger model. But the subtlety comes in on the read side. What happens when the participant, during interpretation, encounters a ``fetch``, ``fetchByKey``, or ``lookupByKey``?

To illustrate the problem, let's assume there is a template ``T`` with a contract key, and Alice has witnessed two ``Create`` nodes of a contract of type ``T`` with key ``k``, but no corresponding archive nodes. Alice may not be able to order these two nodes causally in the sense of "one create came before the other". See :doc:`/concepts/local-ledger` for an in-depth treatment of causality on DAML Ledgers.

So what should happen now if Alice's participant encounters a ``fetchByKey @T k`` or ``lookupByKey @T k`` during interpretation? What if it encounters a ``fetch`` node? These decisions are part of the operational semantics, and the decision of what should happen is based on the consideration that the chance of a participant submitting an invalid transaction should be minimized.

If a ``fetch`` or ``exercise`` is encountered, the participant resolves the contract as long as it has not witnessed an archive node for that contract - ie as long as it can't guarantee that the contract is no longer active. The rationale behind this is that ``fetch`` and ``exercise`` use ContractIds, which need to come from somewhere: Command arguments, Contract arguments, or key lookups. In all three cases, someone believes the ContractId to be active still so it's worth trying.

If a ``fetchByKey`` or ``lookupByKey`` node is encountered, the contract is only resolved if the requester is a stakeholder on an active contract with the given key. If that's not the case, there is no reason to believe that the key still resolves to some contract that was witnessed earlier. Thus, when using contract keys, make sure you make the likely requesters of transactions observers on your contracts. If you don't, ``fetchByKey`` will always fail, and ``lookupBeyKey`` will always return ``None``.

Let's illustrate how collisions and operational semantics and interleave:

1. Bob creates ``T`` with key ``k``. Alice is not a stakeholder.
2. Alice submits a command resulting in well-authorized ``lookupByKey @T k`` during interpretation. Even if Alice witnessed 1, this will resolve to a ``None`` as Alice is not a stakeholder. This transaction is invalid at the time of interpretation, but Alice doesn't know that.
3. Bob submits an ``exerciseByKey @T k Archive``.
4. Depending on which of the transactions from 2 and 3 gets sequenced first, either just 3, or both 2 and 3 get committed. If 3 is committed before 2, 2 becomes valid while in transit.

As you can see, the behavior of ``fetch``, ``fetchByKey`` and ``lookupByKey`` at interpretation time depend on what information is available to the requester at that time. That's something to keep in mind when writing DAML contracts, and something to think about when encountering frequent "Disputed" errors.

Next up
-------

You've reached the end of the Introduction to DAML. Congratulations. If you think you understand all this material, you could test yourself by getting DAML certified at `https://academy.daml.com <https://academy.daml.com>`__. Or put your skills to good use by developing a DAML application. There are plenty of examples to inspire you on the :doc:`/examples/examples` page.