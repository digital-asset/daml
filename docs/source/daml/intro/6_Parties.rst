.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Parties and Authority
=====================

Daml is designed for distributed applications involving mutually distrusting parties. In a well-constructed contract model, all parties have strong guarantees that nobody cheats or circumvents the rules laid out by templates and choices.

In this section you will learn about Daml's authorization rules and how to develop contract models that give all parties the required guarantees. In particular, you'll learn how to:

- Pass authority from one contract to another
- Write advanced choices
- Reason through Daml's Authorization model

.. hint::

  Remember that you can load all the code for this section into a folder called ``intro6`` by running ``daml new intro6 --template daml-intro-6``

Preventing IOU Revocation
-------------------------

The ``SimpleIou`` contract from :doc:`4_Transformations` and :doc:`5_Restrictions` has one major problem: The contract is only signed by the ``issuer``. The signatories are the parties with the power to create and archive contracts. If Alice gave Bob a ``SimpleIou`` for $100 in exchange for some goods, she could just archive it after receiving the goods. Bob would have a record of such actions, but would have to resort to off-ledger means to get his money back:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- SIMPLE_IOU_BEGIN
  :end-before: -- SIMPLE_IOU_END

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- SIMPLE_IOU_SCENARIO_BEGIN
  :end-before: -- SIMPLE_IOU_SCENARIO_END

For a party to have any guarantees that only those transformations specified in the choices are actually followed, they either need to be a signatory themselves, or trust one of the signatories to not agree to transactions that archive and re-create contracts in unexpected ways. To make the ``SimpleIou`` safe for Bob, you need to add him as a signatory:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- IOU_BEGIN
  :end-before: -- IOU_END

There's a new problem here: There is no way for Alice to issue or transfer this ``Iou`` to Bob. To get an ``Iou`` with Bob's signature as ``owner`` onto the ledger, his authority is needed:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- IOU_SCENARIO_BEGIN
  :end-before: -- IOU_SCENARIO_END

This may seem awkward, but notice that the ``ensure`` clause is gone from the ``Iou`` again. The above ``Iou`` can contain negative values so Bob should be glad that ``Alice`` cannot put his signature on any ``Iou``.

You'll now learn a couple of common ways of building issuance and transfer workflows for the above ``Iou``, before diving into the authorization model in full.


.. _intro propose accept:

Use Propose-Accept Workflows for One-Off Authorization
------------------------------------------------------

If there is no standing relationship between Alice and Bob, Alice can propose the issuance of an Iou to Bob, giving him the choice to accept. You can do so by introducing a proposal contract ``IouProposal``:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- IOU_PROPOSAL_BEGIN
  :end-before: -- IOU_PROPOSAL_END

Note how we have used the fact that templates are records here to store the ``Iou`` in a single field:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- IOU_PROPOSAL_SCENARIO_BEGIN
  :end-before: -- IOU_PROPOSAL_SCENARIO_END

The ``IouProposal`` contract carries the authority of ``iou.issuer`` by virtue of them being a signatory. By exercising the ``IouProposal_Accept`` choice, Bob adds his authority to that of Alice, which is why an ``Iou`` with both signatories can be created in the context of that choice.

The choice is called ``IouProposal_Accept``, not ``Accept``, because propose-accept patterns are very common. In fact, you'll see another one just below. As each choice defines a record type, you cannot have two choices of the same name in scope. It's a good idea to qualify choice names to ensure uniqueness.

The above solves issuance, but not transfers. You can solve transfers exactly the same way, though, by creating a ``TransferProposal``:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- TRANSFER_PROPOSAL_BEGIN
  :end-before: -- TRANSFER_PROPOSAL_END

In addition to defining the signatories of a contract, ``signatory`` can also be used to extract the signatories from another contract. Instead of writing ``signatory (signatory iou)``, you could write ``signatory iou.issuer, iou.owner``.

The ``IouProposal`` had a single signatory so it could be cancelled easily by archiving it. Without a ``Cancel`` choice, the ``newOwner`` could abuse an open TransferProposal as an option. The triple ``Accept``, ``Reject``, ``Cancel`` is common to most proposal templates.

To allow an ``iou.owner`` to create such a proposal, you need to give them the choice to propose a transfer on the ``Iou`` contract. The choice looks just like the above ``Transfer`` choice, except that a ``IouTransferProposal`` is created instead of an ``Iou``:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- PROPOSE_TRANSFER_BEGIN
  :end-before: -- PROPOSE_TRANSFER_END

Bob can now transfer his ``Iou``. The transfer workflow can even be used for issuance:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- IOU_TRANSFER_SCENARIO_BEGIN
  :end-before: -- IOU_TRANSFER_SCENARIO_END

Use Role Contracts for Ongoing Authorization
--------------------------------------------

Many actions, like the issuance of assets or their transfer, can be pre-agreed. You can represent this succinctly in Daml through relationship or role contracts.

Jointly, an ``owner`` and ``newOwner`` can transfer an asset, as demonstrated in the script above. In :doc:`7_Composing`, you will see how to compose the ``ProposeTransfer`` and ``IouTransferProposal_Accept`` choices into a single new choice, but for now, here is a different way. You can give them the joint right to transfer an IOU:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- MUTUAL_TRANSFER_BEGIN
  :end-before: -- MUTUAL_TRANSFER_END

Up to now, the controllers of choices were known from the current contract. Here, the ``newOwner`` variable is part of the choice arguments, not the ``Iou``.

This is also the first time we have shown a choice with more than one controller. If multiple controllers are specified, the authority of *all* the controllers is needed. Here, neither ``owner``, nor ``newOwner`` can execute a transfer unilaterally, hence the name ``Mutual_Transfer``.

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- SENDER_ROLE_BEGIN
  :end-before: -- SENDER_ROLE_END

The above ``IouSender`` contract now gives one party, the ``sender`` the right to send ``Iou`` contracts with positive amounts to a ``receiver``. The ``nonconsuming`` keyword on the choice ``Send_Iou`` changes the behaviour of the choice so that the contract it's exercised on does not get archived when the choice is exercised. That way the ``sender`` can use the contract to send multiple Ious.

Here it is in action:

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- SENDER_SCENARIO_BEGIN
  :end-before: -- SENDER_SCENARIO_END

Daml's Authorization Model
--------------------------

Hopefully, the above will have given you a good intuition for how authority is passed around in Daml. In this section you'll learn about the formal authorization model to allow you to reason through your contract models. This will allow you to construct them in such a way that you don't run into authorization errors at runtime, or, worse still, allow malicious transactions.

In :ref:`choices` you learned that a transaction is, equivalently, a tree of transactions, or a forest of actions, where each transaction is a list of actions, and each action has a child-transaction called its consequences.

Each action has a set of *required authorizers* -- the parties that must authorize that action -- and each transaction has a set of *authorizers* -- the parties that did actually authorize the transaction.

The authorization rule is that the required authorizers of every action are a subset of the authorizers of the parent transaction.

The required authorizers of actions are:

- The required authorizers of an **exercise action** are the controllers on the corresponding choice. Remember that ``Archive`` and ``archive`` are just an implicit choice with the signatories as controllers.
- The required authorizers of a **create action** are the signatories of the contract.
- The required authorizers of a **fetch action** (which also includes ``fetchByKey``) are somewhat dynamic and covered later.

The authorizers of transactions are:

- The root transaction of a commit is authorized by the submitting party.
- The consequences of an exercise action are authorized by the actors of that action plus the signatories of the contract on which the action was taken.

An Authorization Example
~~~~~~~~~~~~~~~~~~~~~~~~

Consider the transaction from the script above where Bob sends an ``Iou`` to Charlie using a ``Send_Iou`` contract.
It is authorized as follows, ignoring fetches:

- Bob submits the transaction so he's the authorizer on the root transaction.
- The root transaction has a single action, which is to exercise ``Send_Iou`` on a ``IouSender`` contract with Bob as ``sender`` and Charlie as ``receiver``. Since the controller of that choice is the ``sender``, Bob is the required authorizer.
- The consequences of the ``Send_Iou`` action are authorized by its actors, Bob, as well as signatories of the contract on which the action was taken. That's Charlie in this case, so the consequences are  authorized by both Bob and Charlie.
- The consequences contain a single action, which is a ``Mutual_Transfer`` with Charlie as ``newOwner`` on an ``Iou`` with ``issuer`` Alice and ``owner`` Bob. The required authorizers of the action are the ``owner``, Bob, and the ``newOwner``, Charlie, which matches the parent's authorizers.
- The consequences of ``Mutual_Transfer`` are authorized by the actors (Bob and Charlie), as well as the signatories on the Iou (Alice and Bob).
- The single action on the consequences, the creation of an Iou with ``issuer`` Alice and ``owner`` Charlie has required authorizers Alice and Charlie, which is a proper subset of the parent's authorizers.

You can see the graph of this transaction in the transaction view of the IDE:

.. code-block:: none

  TX 12 1970-01-01T00:00:00Z (Parties:276:3)
  #12:0
  │   disclosed to (since): 'Bob' (12), 'Charlie' (12)
  └─> 'Bob' exercises Send_Iou on #10:0 (Parties:IouSender)
            with
              iouCid = #11:3
      children:
      #12:1
      │   disclosed to (since): 'Bob' (12), 'Charlie' (12), 'Alice' (12)
      └─> fetch #11:3 (Parties:Iou)

      #12:2
      │   disclosed to (since): 'Bob' (12), 'Charlie' (12), 'Alice' (12)
      └─> 'Bob', 'Charlie' exercises Mutual_Transfer on #11:3 (Parties:Iou)
                           with
                             newOwner = 'Charlie'
          children:
          #12:3
          │   disclosed to (since): 'Bob' (12), 'Charlie' (12), 'Alice' (12)
          └─> create Parties:Iou
              with
                issuer = 'Alice';
                owner = 'Charlie';
                cash =
                  (Parties:Cash with
                     currency = "USD"; amount = 100.0000000000)

Note that authority is not automatically transferred transitively.

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- NON_TRANSITIVE_BEGIN
  :end-before: -- NON_TRANSITIVE_END

.. literalinclude:: daml/daml-intro-6/daml/Parties.daml
  :language: daml
  :start-after: -- NON_TRANSITIVE_SCENARIO_BEGIN
  :end-before: -- NON_TRANSITIVE_SCENARIO_END

The consequences of ``TryB`` are authorized by both Alice and Bob, but the action ``TryA`` only has Alice as an actor and Alice is the only signatory on the contract.

Therefore, the consequences of ``TryA`` are only authorized by Alice. Bob's authority is now missing to create the flipped ``NonTransitive`` so the transaction fails.

Next Up
-------

In :doc:`7_Composing` you will put everything you have learned together to build a simple asset holding and trading model akin to that in the :doc:`/app-dev/bindings-java/quickstart`. In that context you'll learn a bit more about the ``Update`` action and how to use it to compose transactions, as well as about privacy on Daml ledgers.
