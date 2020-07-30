.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0
   
.. _interoperable-ledgers:

Ledger Interoperability
#######################

Certain DAML ledgers can interoperate with other DAML ledgers.
That is, the contracts created on one ledger can be used and archived in transactions on other ledgers.
Participant Nodes connect to multiple ledgers and provide their parties unified access to those ledgers via the :ref:`Ledger API <ledger-api-services>`.
For example, when an organization initially deploys two workflows to two DAML ledgers, it can later compose those workflows into a larger workflow that spans both ledgers.

This section covers the following topics:

- The :ref:`topologies <interoperable-topology>` for interoperable ledgers

- The :ref:`aggregation <interoperable-aggregation>` of interoperable ledgers into the local ledgers exposed over the Ledger API

- The :ref:`causality guarantees <interoperable-causality>` for interoperable ledgers.


The presentation assumes that you are familiar with the following concepts:

- :ref:`Local ledgers and causality <local-ledger>`
- The :ref:`Ledger API <ledger-api-services>`
- The :ref:`DAML ledger model <da-ledgers>`

.. _interoperable-topology:

Topologies
**********

Participant Nodes connect to DAML ledgers and parties access to projections of these ledgers via the Ledger API.
The following picture shows such a setup.

.. figure:: ./images/multiple-domains.svg
   :align: center
   :name: multiple-ledgers

   Example topology with three interoperable ledgers

The components in this diagram are the following:

* There is a set of interoperable **DAML ledgers**: Ledger 1, Ledger 2, and Ledger 3.

* Each **Participant Node** is connected to a subset of the DAML ledgers.
  
  - Participant Node 1 is connected to Ledger 1 and 2.
  - Participant Nodes 2 and 3 are connected to Ledger 1, 2, and 3.

* Participant Nodes host parties on a subset of the DAML ledgers they are connected to.
  A Participant Node provides a party access to the DAML ledgers that it hosts the party on.

  - Participant Node 1 hosts Alice on Ledger 1 and 2.
  - Participant Node 2 hosts Alice on Ledger 1 and 3, but not on 2 (indicated by the dotted connection).
    Alice can thus access Ledger 2 only through Participant Node 1, even though Participant Node 2 is also connected.
  - Participant Node 3 hosts Bob on Ledger 1, 2, and 3.

.. _interoperable-aggregation:

Aggregation into Local Ledgers
******************************

When a Participant Node hosts a party only on a subset of the interoperable DAML ledgers,
then the transaction and active contract services of the Participant Node are derived only from those ledgers.
In practice, the Participant Node assembles the updates from these ledgers into the party's local ledger,
which feeds the Transaction Service and Active Contract Service for this party.

In the :ref:`above example <multiple-ledgers>`, when a transaction creates a contract with stakeholder `A` on Ledger 2,
then this transaction is in `P1`\ 's local ledger for `A`, but not in `P2`\ 's.
Accordingly, `P1` outputs the transaction and the ``CreatedEvent`` on `A`\ 's event stream and reports the contract as active.
In contrast, `P2` will neither output the event nor report the contract as active, as `P1` is not connected to domain 2.

Contracts Entering and Leaving Local Ledgers
============================================

For interoperability, it is important that a transaction can use a contract whose creation comes from a different ledger.
In the :ref:`above example <multiple-ledgers>`, one transaction can create a contract with stakeholder `A` on Ledger 1 and another archives the contract on Ledger 2.
Then endpoint `P2` outputs the ``CreatedEvent``, but not the ``ArchiveEvent`` on the transaction service
because Ledger 2 need not notify `P2` about updates that pertain to Alice.
Conversely, when one transaction creates a contract with stakeholder Alice on Ledger 3 and another archives the contract on Ledger 1, then `P1` outputs the ``ArchivedEvent``, but not the ``CreateEvent``.

To keep the transaction stream consistent, `P2` additionally outputs a ``LeaveLocalLedgerEvent`` on Alice's transaction stream.
This event signals that the endpoint no longer outputs events concerning this contract; in particular not when the contract is archived.
The contract is no longer reported in the active contract service and cannot be used by command submissions.

Conversely, `P1` outputs an ``EnterLocalLedgerEvent``\ s some time before the ``ArchivedEvent`` on the transaction stream.
This event signals that the endpoint starts outputting events concerning this contract.
The contract is reported in the active contract service and can be used by command submission.
The ``EnterLocalLedgerEvent`` contains all the information in a ``CreatedEvent``;
the only difference is that ``EnterLocalLedgerEvent``\ s may occur several times whereas there should be at most one ``CreatedEvent`` for each contract.

These events are generated when the underlying interoperability protocol synchronizes between the different ledgers.
This may happen as part of command submission or for other reasons, e.g., load balancing.
It is guaranteed that the ``EnterLocalLedgerEvent`` precedes contract usage, subject to the trust assumptions of the underlying ledgers and the interoperability protocol.

A contract may enter and leave the local ledger of a Participant Node several times.
For example, suppose that Bob submits the following commands and their commits end up on the given ledgers.

#. Create a contract `c` with signatories Alice and Bob on Ledger 2
#. Exercise a non-consuming choice `ch1` on `c` on Ledger 1.
#. Exercise a non-consuming choice `ch2` on `c` on Ledger 2.
#. Exercise a consuming choice on `c` on Ledger 1.

Then, the transaction tree stream that `P2` provides for `A` contains five events involving contract `c`: ``EnterLocalLedgerEvent``, ``ExercisedEvent``, ``LeaveLocalLedgerEvent``, ``EnterLocalLedgerEvent``, ``ArchivedEvent``.
These five events can be grouped into between two or five transactions.
For example, the first three and the last two could be batched into one transaction each.
However, `P2` cannot combine the ``LeaveLocalLedgerEvent`` with the subsequent ``EnterLocalLedgerEvent`` and `P2` must not elide them either.
This is because their presence indicates that `P2`\ 's local ledger for Alice may miss some events in between; in this example, exercising the choice `ch2`.

The flat transaction stream by `P2` shows omits the non-consuming exercise choices.
It nevertheless contains the three view change events ``EnterLocalLedgerEvent``, ``LeaveLocalLedgerEvent``, and ``EnterLocalLedgerEvent`` before the ``ArchivedEvent``.
This is because the endpoint cannot know at the ``LeaveLocalLedgerEvent`` that there will be another ``EnterLocalLedgerEvent``.

In contrast, `P1` need not output the ``EnterLocalLedgerEvent``\ s and ``LeaveLocalLedgerEvent``\ s at all in this example because `P1` hosts `A` on both ledgers.


Local Ledgers with Enter and Leave Actions
==========================================

The ``EnterLocalLedgerEvent`` and ``LeaveLocalLedgerEvent`` events are included in the transactions of the local ledger that the participant maintains for a party.
They are abbreviated as **Enter** and **Leave** actions.
**Enter** counts as a contract activation and **Leave** as a contract deactivation.
The :ref:`local ledger structure <local-ledger-structure>` for interoperable ledgers generalizes as follows.

The transactions in the local ledger contain all the actions of the DAML Ledger Model and additionally the ``EnterLocalLedgerEvent`` and ``LeaveLocalLedgerEvent`` events,
which are referred to as **Enter** and **Leave** actions.
As before, the stream of transactions and transaction trees are derived as a topological sort of the local ledger for the set of parties.

It suffices to extend :ref:`definition of activation and deactivation <local-ledger-structure>` actions as follows.
The actual definition of local ledger remains the same except that every local ledger implicitly identifies the set of DAML ledgers it aggregates.

Definition »activation, deactivation«

  * A **Enter** action on a contract **activates the contract**.

  * A **Leave** action on a contract **deactivates the contract**.


.. _interoperable-causality:

Causality for Interoperable Ledgers
***********************************

The :ref:`ordering guarantees <order-consistency>` between the local ledger and the transaction service does not need to be changed for interoperability.
The order consistency of several local ledgers and the shared ledger, however, need to be generalized for interoperability.

The virtual shared ledger
=========================

While every DAML ledger may keep a physical copy of its shared ledger,
there is no place that records the result of the interoperability protocol between those ledgers;
it merely ensures that the individual shared ledgers are consistent.
Like for local ledgers, consistency is formalized by the existence of a *virtual* shared ledger.

Definition »virtual shared ledger«
  A **virtual shared ledger** for a set `X` of interoperable DAML ledgers
  is a finite labelled directed acyclic graph of transactions that satisfies the following conditions:

  * It satisfies the conditions for a shared ledger.

  * It does not contain **Enter** nor **Leave** actions.

  * Every action in the vertex transactions is labelled with a DAML ledger from `X`.

Since a cross-ledger transaction can use contracts from different DAML ledgers,
the :ref:`projection <da-model-projections>` of transactions needs to be aware of the ledgers.
In the `PaintOffer` workflow, e.g., Alice's and the painter's projections of the acceptance transactions are the whole transaction
as they are both stakeholders on the `PaintOffer` contract.
When this transaction is run across two ledgers, say one ledger for dealing with `Iou`\ s and one ledger for dealing with painting,
Alice's and the painter's participants on the `Iou` ledger will see only the `Iou` part of the transaction.
Accordingly, the ledger-aware projections look as follows, where yellow represents the `Iou` ledger and blue stands for the painting ledger.
Here, the projections to the blue ledger include the actions of the yellow ledger because a projection includes the subactions.

.. https://www.lucidchart.com/documents/edit/f8ec5741-7a37-4cf5-92a9-bf7b3132ba8e
.. image:: ./images/projecting-transactions-paint-offer-ledger-aware.svg
   :align: center
   :width: 60%

Definition »Y-labelled action«
  An action is **Y-labelled** for a set `Y` if it is labelled with an element of `Y`.
           
Definition »ledger-aware projection«
  Let `tx` be a transaction according to the DAML Ledger Model whose actions are `X`\ -labelled for a set `X` of DAML ledgers
  and let `Y` be a subset of `X`.
  Let `Act` be the set of `Y`\ -labelled subactions of `tx` that have `A` as an informee.
  The **Y-projection** of `tx` to a party `A` consists of maximal elements of `Act` (w.r.t. the subaction relation) in execution order.

Ledger-aware projections extend to the virtual shared ledger straightforwardly.
  
Interoperable order consistency
===============================
  
The interoperable order consistency between local ledgers is then defined as follows:
  
Definition »interoperable order consistency«
  A set `Ls` of local ledgers is **interoperable order-consistent**
  if there exists a virtual shared ledger `G` for a set of `X` of interoperable DAML ledgers with the following properties:

  * *Closed world:*
    The ledgers in `Ls` aggregate only DAML ledgers in `X`.

  * *Injective agreement:*
    For every local ledgers `L` in `Ls` for party `A` that aggregates a set `Y` of DAML ledgers, the following holds.
    Let `V` be the vertices in `L` that contain actions other than **Enter** and **Leave**
    and let `W` be the vertices in `G` that whose `Y`\ -projection to `A` is not empty.
    Then there is a bijection between `V` and `W` such that removing all **Enter** and **Leave** actions from the transaction in `V` gives the `Y`-projection of the corresponding transaction in `W`.
    Whenver `L` contains an edge between two in `V`, then `G` contains an edge between the corresponding vertices in `W`.

  * *Enter and Leave*:
    For every `Y`\ -labelled action `act` in `G` with informee `A` that uses a contract `c`, the following holds:

    - If there is no `Y`\ -labelled action that precedes `act` in `G`\ 's action order and acts on the same contract,
      then `L` contains an **Enter** action for the contract that precedes `act` in `L`.

    - If there are actions `act`:sub:`1` and `act`:sub:`2` such that `act`:sub:`1` precedes `act`:sub:`2` and `act`:sub:`2` precedes `act` in `G` and `act`:sub:`1` is `Y`\ -labelled, but not `act`:sub:`2`, then `L` contains an **Enter** and a **Leave** action for the contract between `act`:sub:`1` and `act`.

    - For every **Leave** action before `act`, `L` contains an **Enter** action between the **Leave** action and `act`.

.. note::
   There are no ordering guarantees for contract keys other than those that come from the contracts they reference.

Like for a single DAML ledger, interoperable order consistency ensures that the order of transactions is consistent across several Participant Nodes and parties.
Moreover, if a interoperable order-consistent local ledger contains actions on one of `A`\ ;s contracts,
then it delimits with **Leave** and **Enter** when actions on that contract may be missing from that ledger.

These **Leave** and **Enter** actions are not ordered w.r.t. actions on other contracts.
So a local ledger may report a contract as active although it has already been archived on another ledger even if the archival's consequences causally precede other actions on the ledger.

Connection with the DAML Ledger Model
=====================================

The virtual shared ledger can be understood as a ledger of the DAML Ledger Model analogously to a :ref:`shared ledger <connection-ledger-model>`:
topologically sort the vertices and drop the labels on actions.
The interoperability protocol ensures that the virtual shared ledger is valid, subject to the trust assumptions of the underlying ledgers and the protocol.
Accordingly, the local ledgers inherit the validity guarantees from the virtual shared ledger like they do from a shared ledger of a single DAML ledger.
