.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _local-ledger:

Local Ledgers and Causality
###########################

A local ledger represents the view on a DAML ledger that a Participant Node maintains for a party.
Parties access this view via the :ref:`Ledger API <ledger-api-services>`.
This document introduces the concept of a :ref:`local ledger <local-ledger-structure>`
and how it relates to the :ref:`Ledger API <ledger-api-services>` and the :ref:`DAML Ledger Model <da-ledgers>`.

The current version covers the following Ledger API services:

* :ref:`Transaction Service <transaction-service>`
* :ref:`Active Contract Service <active-contract-service>`

It does not yet cover :ref:`submission services <ledger-api-submission-services>`,
:ref:`utility services <ledger-api-utility-services>`,
nor :ref:`testing services <ledger-api-testing-services>`.

Local ledgers capture causality between transactions from a party's perspective
rather than a global ledger perspective, taking the party's limited view on the ledger into account.
When a Participant Node outputs transactions and events to a party,
it respects the causality constraints in the party's view, but it is to ignore causality constraints that are not encoded the DAML models.
Accordingly, DAML ledgers do **not** guarantee that all parties observe all transactions in the same order.

Nevertheless, DAML ledgers ensure that all local ledgers are :ref:`order-consistent <order-consistency>`
and can therefore be combined into a virtual shared ledger that :ref:`connects to the DAML ledger model <connection-ledger-model>`.
The ledger validity guarantees therefore extend via the local ledgers to the Ledger API.

The presentation assumes that you are familiar with the following concepts:

* The :ref:`Ledger API <ledger-api-services>`

* The :ref:`DAML Ledger Model <da-ledgers>`

.. _local-ledger-structure:
   
Structure
*********

The :ref:`Transaction Service <transaction-service>` provides the updates as a stream of DAML transactions
and the :ref:`Active Contract Service <active-contract-service>` summarizes all the updates up to a given point
by the contracts that are active at this point.
Conceptually, both services are derived from the local ledger that the Participant Node manages for each hosted party.

A local ledger for a party `A` at a participant is a directed acyclic graph.
The vertices are transaction trees and the edges describe the partial ordering of the transaction trees.
The stream of transaction trees in the :ref:`Transaction Service <transaction-service>` is some linearization of this partial ordering, as described later.

.. note::
   The transaction trees of the :ref:`Transaction Service <transaction-service>` omit **Fetch** and **NoSuchKey** actions
   that are part of the :ref:`DAML Ledger Model <actions-and-transactions>`.
   A local ledger includes all the actions from the DAML Ledger Model.
   The **Fetch** and **NoSuchKey** actions must therefore be removed before the :ref:`Transaction Service <transaction-service>` outputs the transaction trees.

   The transaction service additionally outputs ``EnterLocalLedgerEvent`` and ``LeaveLocalLedgerEvent`` for interoperable DAML Ledgers.
   This section focuses on a single DAML ledger and therefore abstracts from these events.
   The :ref:`Ledger Interoperability <ledger-interoperability>` section extends the concepts in this section accordingly.

The definition uses the following concepts:

.. _def-action-activation-deactivation:

Definition »activation, use, deactivation, allocation, deallocation«
  Every action allocates, uses, or deactivates a contract or it allocates, uses, or deallocates a key.

  * A **Create** action of a contract **activates the contract**.
    If the created contract has a key, the action also **allocates the key** to the created contract.

  * An **Exercise** action on a contract **uses the contract**.
    If the exericse is consuming, it also **deactivates the contract** and, if the contract has a key, it additionally **deallocates the key**.

  * A **Fetch** action on a contract **uses the contract**.

  * A **NoSuchKey** action on a key **uses the key**.

Definition »action order«
  For a directed acyclic graph of transactions,
  the induced **action order** on the actions in the transactions combines graph-induced order between transactions with the total order of actions inside every transaction.
  It is the least partial order that includes the following ordering relations between two actions `act`:sub:`1` and `act`:sub:`2`:
  
  * `act`:sub:`1` and `act`:sub:`2` belong to the same transaction and `act`:sub:`1` precedes `act`:sub:`2` in the transaction.
  * `act`:sub:`1` and `act`:sub:`2` belong to different transactions in vertices `tx`:sub:`1` and `tx`:sub:`2` and there is a path from `tx`:sub:`1` to `tx`:sub:`2`.

The action order is a partial order on the actions in a directed acyclic graph of transactions.
Moreover, whenever `act`:sub:`1` is action-ordered before another action `act`:sub:`2` in a directed acyclic graph of transactions,
then the graph contains a (possibly empty) path from `act`:sub:`1`\ 's transaction to `act`:sub:`2`\ 's transaction.

Definition »local ledger«
  A **local ledger** for a party `A` is a finite directed acyclic graph that satisfies the following conditions:

  * *Total order on contract changes:*
    For every contract that `A` is a stakeholder of, the action order totally orders all activations and deactivations of the contract.
    In this sequence, activations alternate with deactivations, starting with an activation.

  * *Total order on key changes:*
    For every key that `A` is a maintainer of, the action order totally orders all allocations and deallocations of the key.
    In this sequence, allocations alternate with deallocations, starting with an allocation, and allocation-deallocations pairs refer to the same contract.

  * *Use ordering:*
    If `A` is a stakeholder of a contract and an informee of an action `act`:sub:`u` that uses the contract,
    then `act`:sub:`u` must be ordered after an activation action of the contract.
    Let `act`:sub:`a` be the last activation action of the contract before `act`:sub:`u`.
    Then there must not be a deactivation action `act`:sub:`d` that is ordered after `act`:sub:`a` and before `act`:sub:`u`.
    
    If an action `act`:sub:`u` uses a key that `A` contains a maintainer of
    and there is a key allocation action `act`:sub:`a` ordered before `act`:sub:`u`,
    then there is a deallocation action `act`:sub:`d` for the key ordered after `act`:sub:`a` and before `act`:sub:`u`.

In the use ordering condition, the last activation before a contract use is well-defined
because the graph is finite and all activations are totally ordered by the first condition.
Moreover, a local ledger remains a local ledger if more edges between transaction vertices are added.

The edges in a local ledger induce a partial order on the vertices, the **causality order**.
A transaction `tx`:sub:`1` causally precedes `tx`:sub:`2` if there is a path from `tx`:sub:`1` to `tx`:sub:`2`.
As the examples below will illustrate,
the conditions ensure that the causality order captures at least the causal dependencies between transactions from `A`\ 's perspective.

.. _order-consistency:

Order consistency
*****************

The local ledger constrains the order of the transactions and transaction trees on the :ref:`Transaction Service <transaction-service>`.
In detail, the following conditions hold:

#. The transaction trees on `A`\ 's transaction tree stream are precisely those that are in the local ledger for `A`.
#. The flat transaction stream contains precisely the ``CreatedEvent``\ s and ``ArchivedEvent``\ s
   that correspond to **Create** and consuming **Exercise** actions in transaction trees on the transaction tree stream where `A` is a stakeholder of the affected contract.
#. Whenever the local ledger causally orders two transactions, they appear in the same order on the streams.

Similarly, the active contract service provides the set of contracts that are active at the returned offset according to the Transaction Service streams.
That is, the effects of all events from the transaction event stream are taken into account in the provided set of contracts.
In particular, an application can process all subsequent events from the flat transaction stream or the transaction tree stream without having to take events before the snapshot into account.

A deployed DAML ledger ensures that all the local ledgers of its Participant Nodes are consistent, subject to the ledger's trust assumptions.
Here, we consider only a single DAML ledger.
The general case of :ref:`several interoperable DAML ledgers <interoperable-ledgers>` is discussed :ref:`elsewhere <interoperable-causality>`.
Consistency can be expressed by embedding all the local ledgers into a single graph of transactions, the shared ledger.
A shared ledger satisfies conditions similar to a local ledger, but the conditions do not restrict the orders to a party.

Definition »shared ledgers«
  A **shared ledger** is a finite directed acyclic graph that satisfies the following conditions:

  * *Total order on contract changes:*
    For every contract, the action order totally orders all activations and deactivations of the contract.
    In this sequence, activations alternate with deactivations, starting with an activation.

  * *Total order on key changes:*
    For every key, the action order totally orders all allocations and deallocations of the key.
    In this sequence, allocations alternate with deallocations, starting with an allocation, and allocation-deallocations pairs refer to the same contract.

  * *Use ordering:*
    If an action `act`:sub:`u` uses a contract,
    then `act`:sub:`u` must be ordered after an activation action of the contract.
    Let `act`:sub:`a` be the last activation action of the contract before `act`:sub:`u`.
    Then there must not be a deactivation action `act`:sub:`d` that is ordered after `act`:sub:`a` and before `act`:sub:`u`.
    
    If an action `act`:sub:`u` uses a key
    and there is a key allocation action `act`:sub:`a` ordered before `act`:sub:`u`,
    then there is a deallocation action `act`:sub:`d` for the key ordered after `act`:sub:`a` and before `act`:sub:`u`.

Definition »order consistency«
  A set `Ls` of local ledgers is **order-consistent** for a single DAML Ledger
  if there exists a shared ledger `G` with the following properties:

  * Whenever `Ls` contains a local ledger `L` for party `A`,
    the vertices of `L` are exactly the vertices of `G` projected to `A` unless the projection is empty.
    (This takes multiplicities into account, i.e., if the projections of two vertices in `G` are the same,
    `L` contains two vertices with the same transactions and it is fixed which vertex in `L` belongs to which vertex in `G`.)
    Moreover, whenever `L` contains an edge between two vertices, `G` contains an edge between the corresponding vertices in `G`.

Order consistency between local ledgers ensures that the order of transactions is *consistent* across several Participant Nodes and between parties.
Consistency does not mean, however, that everyone observe the same transactions in the same order.
The :ref:`causality examples <causality-examples>` below illustrate the fine points.

.. _connection-ledger-model:

Connection to the DAML Ledger Model
***********************************

A DAML ledger provides better guarantees than order consistency, which are discussed now.
The shared ledger derived from local ledgers is a acylcic directed graph of transactions
whereas the Ledger Model formalizes ledgers as a list of commits.
Fortunately, the shared ledger can be understood as such a list of commits:
sort the vertices topologically and pair each transaction with its submitter.

This allows us to express the DAML ledger guarantees in terms of the Ledger Model.
Namely, every DAML ledger ensures (subject to the ledger's trust assumption)
that there exists a shared ledger for all local ledgers
such that this shared ledger is :ref:`valid <da-model-validity>` in the sense of the Ledger Model.
(Note that ledger validity does not depend on the particular topological sort that is chosen.)
In particular, all transactions on the shared ledger are well-authorized and conform to the DAML model.
And so are all transactions on the :ref:`Transaction Service <transaction-service>`
as they are projections of the transactions on the shared ledger.

Transferring consistency to the local ledgers and the transaction stream is trickier as consistency depends on the order of the transactions.
Not all DAML ledgers enforce the same order at all Participant Nodes because this incurs synchronization, which hinders scalability.
Participant Nodes are therefore allowed to output events in different orders if these events do not causally depend on each other from the party's perspective.
Causality is implicitly defined by the conditions for a local ledger.
The examples in the next section will bring these conditions to life.

These reorderings need not happen on any particular DAML ledger, though.
In a :ref:`fully centralized DAML ledger <fully-centralized-ledger>`,
there is by definition only one system that contains a physical copy of the entire shared ledger.
Accordingly, such a ledger can easily serve all transactions to all parties in the same order.
Conversely, :ref:`partitioned ledger topologies <partitioned-topologies>` might need synchronization to reach consensus on the global order of transactions.
Reorderings are therefore more likely to be seen there.

.. _causality-examples:

Causality examples
******************

This section illustrates the ordering guarantees of the Ledger API by example.
The examples use the paint counteroffer workflow from the DA ledger model's :ref:`privacy section <da-model-privacy>`.
Recall that :ref:`the party projections <da-paint-counteroffer-example>` are as follows:

.. https://www.lucidchart.com/documents/edit/c4df0455-13ab-415f-b457-f5654c2684be
.. image:: ./ledger-model/images/divulgence-for-disclosure-counteroffer.svg
   :align: center
   :width: 100%

#. *When two parties are stakeholders on the same contract,
   then they see creation and archival in the same order.*

   This is because creation and archival are activations and deactivations of the same contract.
   The local ledgers for the two parties must therefore determine an order between them because they are stakeholders of the contract.
   Since the local ledgers of a DAML ledger are consistent, they must have ordered them in the same way.
   
   In the example, the creation of the `CounterOffer A P Bank` is ordered before the painter exercising the consuming choice on the `CounterOffer`.
   (If the **Create** was ordered after the **Exercise**, the resulting shared ledger would be inconsistent, which violates the validity guarantee of DAML ledgers.)
   Accordingly, Alice will see the creation before the archival on her transaction stream and so will the painter.
   This does not depend on whether they are hosted on the same Participant Node.

#. *Actions within a commit cannot be reordered.*

   By the *use ordering* rule, Alice's and the Bank's local ledgers must order the **Fetch** of the `Iou` in Alice's commit after the **Create** of the `Iou`.
   Alice will therefore see the creation of the `Iou` before the creation of the `CounterOffer`,
   because the `CounterOffer` is created in the same commit as the **Fetch** of the `Iou`.
   
#. *Out-of-band causality is not respected.*

   The following examples assume that Alice splits up her commit into two as follows:

   .. image:: ./images/counteroffer-split-commit.svg
      :align: center
      :width: 100%
   
   Alice can specify in the `CounterOffer` the Iou that she wants to pay the painter with.
   In an deployed implementation, Alice's application first observes the created `Iou` contract via the transaction service or active contract service before she requests to create the `CounterOffer`.
   Such application logic does not induce a causal ordering between commits.
   So the creation of the `CounterOffer` is not causally ordered w.r.t. the creation of the `Iou`.

   Indeed, no constraint induces an edge in Alice's local ledger between the transaction that creates the `Iou` and the transaction that creates the `CounterOffer`.
   If Alice is hosted on several Participant Nodes, the Participant Nodes can therefore output the two creations in either order.
   
   The rationale for this behaviour is that Alice could have learnt about the contract ID out of band or made it up.
   The Participant Nodes therefore cannot know whether there will ever be a **Create** event for the contract.
   So if Participant Nodes delayed outputting the **Create** action for the `CounterOffer` until a **Create** event for the `Iou` contract was published,
   this delay might last forever and liveness is lost.
   Causality therefore does not capture data flow through applications.
   
#. *Divulged actions do not induce causal order.*

   The painter witnesses the fetching of Alice's `Iou` when the `ShowIou` contract is archived.
   The painter also witnesses archival of the `Iou` when Alice exercises the transfer choice as a consequence of the painter accepting the `CounterOffer`.
   However, the *use ordering* rule does not apply as the painter is not a stakeholder of the `Iou`.
   
   Consider a setup where two Participant Nodes `N`:sub:`1` and `N`:sub:`2` host the painter.
   He sees the divulged `Iou` and the created `CounterOffer` through `N`:sub:`1`\ 's transaction tree stream
   and then submits the acceptance through `N`:sub:`1`.
   As in the previous example, `N`:sub:`2` does not know about the dependence of the two commits.
   Therefore, the painter's local ledgers on `N`:sub:`2` need not order the two transactions.
   Accordingly, `N`:sub:`2` may output the accepting transaction *before* the `ShowIou` contract on the transaction stream.
      
   Even though this may seem unexpected, it is in line with stakeholder-based ledgers:
   Since the painter is not a stakeholder of the `Iou` contract, he should not care about the archivals or creates of the contract.
   In fact, the divulged `Iou` contract does not show up in the painter's active contract service or in the flat transaction stream.
   Such witnessed events are included in the transaction tree stream as a convenience:
   They relieve the painter from computing the consequences of the choice and enable him to check that the action conforms to the model.

   By a similar argument, being an actor of an **Exercise** action induces causal order with respect to other uses of the contract only if the actor is a contract stakeholder.
   This is because non-stakeholder actors of an **Exercise** action authorize the action, but they have no say in whether the contract is active; this is the signatories' job.
   
#. *Causality depends on the party.*

   By the previous example, for the painter, fetching the `Iou` is not causally ordered before transferring the `Iou`.
   For Alice, however, the **Fetch** is causally ordered before the **Exercise** by the *use ordering* rule
   because Alice is a stakeholder on the `Iou` contract.
   This shows that causal ordering depends on the party.
   Even if both Alice and the painter are hosted on the same Participant Node,
   the acceptance transaction can precede the `ShowIou` transaction in the painter's transaction stream.
