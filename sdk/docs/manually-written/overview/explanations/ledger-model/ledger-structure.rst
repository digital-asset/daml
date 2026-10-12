.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

   

   
.. _ledger-structure:

Structure
#########

This section looks at the structure of a ledger that records the interactions between the parties as ledger changes.
The definitions presented here address the first question: "What do changes and ledgers look like?".
The basic building blocks of the recorded interactions are :ref:`actions <actions>`, which get grouped into :ref:`transactions <transactions>`, *updates*, *commits*, and the :ref:`Ledger <da-ledger-definition>`.

.. _ledger-structure_running_example:

Running workflow example
************************

Most of the examples in this section look at the following Daml Script scenarios
based on the templates from the :ref:`running example <da-ledgers-running-example>`.
Two banks first each issue one asset to either Alice or Bob and then Alice proposes a DvP to Bob.
Bob accepts the proposal and settles the DvP.

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-SCRIPT-BEGIN
   :end-before: SNIPPET-SCRIPT-END

Acceptance and settlement can happen either in a single step via the ``AcceptAndSettle`` choice:

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-ACCEPT_AND_SETTLE-BEGIN
   :end-before: SNIPPET-ACCEPT_AND_SETTLE-END

or in two separate steps with ``Accept`` followed by ``Settle``:

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-ACCEPT_THEN_SETTLE-BEGIN
   :end-before: SNIPPET-ACCEPT_THEN_SETTLE-END


.. _actions:

Actions
*******

.. _actions-hierarchical-structure:

Hierarchical structure
======================

One of the main features of the Ledger Model is a *hierarchical action structure*.
This structure is illustrated using Bob settling the DvP by exercising the ``Settle`` choice in the above scenario.
Alice and Bob have allocated their assets (contracts #1 and #2) on the ledger to a ``SimpleDvp`` contract (#4).
These contracts appears as inputs (dashed boxes on the left) in the diagram below.

.. https://lucid.app/lucidchart/f3f49c7c-d257-4136-9dcb-39750f45c24d/edit
.. image:: ./images/dvp-settle-action.svg
   :alt: The settlement action on the ``SimpleDvp`` contract between Alice and Bob, with the two legs of the swap as consequences.
         
Exercising the ``Settle`` choice yields an Exercise action,
which is the tree of nodes shown in blue.
The input contracts on the left are not part of the action.
The root node describes the parameters of the choice and references the ``SimpleDvp`` input contract #4.
It has two subtrees, which perform the asset transfers automatically as part of the ``Settle`` choice.

#. The left subtree represents Alice exercising the ``Transfer`` choice on her ``SimpleAsset`` contract #1.
   It consists of two nodes:
   The root node describes the parameters of the choice and the input contract #1.
   The child node, which is a one-node subtree of its own, encodes the creation of Bob's new ``SimpleAsset`` contract #5.
   
#. The right subtree is analogous:
   The root node of the subtree describes Bob exercising the ``Transfer`` choice on his ``SimpleAsset`` contract #2,
   and its child encodes the creation of Alice's new ``SimpleAsset`` contract #6.

Notably, the Exercise action is the whole tree even though the root node already describes all the relevant parameters.
The Ledger Model focuses on actions rather than nodes because the root node cannot exist on its own, without its children,
as the choice body in the Daml model must always execute when the choice is exercised.
The :ref:`integrity section <da-model-integrity>` goes into the details of this.

Nevertheless actions are not indivisible, but hierarchical:
The left and right subtrees are actions in their own right,
namely the Exercise actions for Alice and Bob exercising their ``Transfer`` choice on their ``SimpleAsset`` input contracts #1 and #2, respectively.
And each of the two subtrees contains another subtree,
namely the creation of Bob's and Alice's new ``SimpleAsset`` contracts #5 and #6.
Each of these subtrees is an action in its own right.
This hierarchical structure induces a :ref:`subaction relationship explained below <da-ledger-subaction>`
and forms the basis for the :ref:`privacy model <da-model-privacy>`.


.. _def-action:

Definition
==========

Overall, the settlement in the above example contains two types of actions:

#. Creating contracts

#. Exercising choices on contracts.

These are also the two main types of actions in the Ledger Model.

To understand the ledger structure, we must distinguish between an action node (the discrete event data) and an action (the entire execution tree).

An action node or simply a **node** is one of the following:

#. A **Create** node records the creation of the contract.
   It contains the following pieces of information:

   * The **contract ID** is a unique identifier of the contract.
     It is equivalent to the transaction output (TxO) in ledgers based on :ref:`unspent transaction outputs (UTxO) <da-ledger-input-output>`.

   * The **template ID** identifies the Daml code associated with the contract,
     and its arguments define the **contract instance**, which is the immutable data associated with the contract ID.

   * The **signatories** are the non-empty set of parties that must authorize the creation and archival of the contract.

   * The **contract observers**, or just observers for short, are the set of parties that will be informed about the contract creation and archival, in addition to the signatories.

In Daml, the signatories and contract observers are determined by the ``signatory`` and ``observer`` clauses defined by the template.
   
   Create nodes are depicted as shown below.
   Diagrams often omit fields with empty values and observers that are also signatories.

   .. https://lucid.app/lucidchart/31888b88-d836-457d-a4a8-05e3e161e07f/edit
.. image:: ./images/create-node.svg
:align: center
:width: 30%
:alt: The structure of a **Create** node.

#. An **Exercise** node records the parameters of a choice that one or more parties have exercised on a contract.
   It contains the following pieces of information:

   * An exercise **kind**, which is either **consuming** or
     **non-consuming**. Once consumed, a contract cannot be used (i.e. fetched or subject to Exercise actions on) again;
     for example, Alice must not be able to transfer her asset twice, as this would be double spending.
     In contrast, contracts exercised in a non-consuming fashion can be reused, for example for expressing a delegation from one party to another.
      
   * The **contract ID** on which the choice is exercised.
     This contract is called the **input contract**.

   * The **interface ID** if this choice was exercised through a Daml interface.

   * The **template ID** that defines the smart contract code for the choice with the given **choice name**,
     and the **choice arguments** that are passed to the smart contract code.
     
   * An associated set of parties called **actors**.
     These are the parties who perform the action.
     They are specified in the ``controller`` clause in the Daml template.

   * An associated set of **choice observers**.
     These parties will be informed about the choice being exercised.

   * The **exercise result** as the Daml value returned by evaluating the choice body.

   Exercise nodes are depicted as shown below, where the consequences are indicated by arrows ordered left-to-right.
   Diagrams omit the kind if it is consuming, empty field values, and choice observers that are also actors.

   .. https://lucid.app/lucidchart/ce3c7eb2-081e-4ac4-af92-5efc11d21c17/edit
.. image:: ./images/exercise-node.svg
:align: center
:width: 30%
:alt: The structure of an **Exercise** node.

#. A **Fetch** node acting on a contract, which demonstrates that the contract exists and is active at the time of fetching.
   A Fetch behaves like a non-consuming Exercise with no consequences, and can be repeated.
   The fetch node contains the following pieces of information, analogous to Exercise nodes:
   **contract ID**, **interface ID**, **template ID**, and the **actors**, namely the parties who fetch the contract.

   Fetch nodes are depicted as shown below.

   .. https://lucid.app/lucidchart/27844d5e-0cdb-4f22-8f67-e97f3839e613/edit
.. image:: ./images/fetch-node.svg
:align: center
:width: 30%
:alt: The structure of a **Fetch** node.

Later, in the :ref:`causality section <local-ledger>`, we will also encounter another type of node called NoSuchKey.

An **action** consists of a **root node** and an ordered list of **consequences**, which are themselves actions that get triggered as a result of the execution of the action at the root node.
This gives rise to the tree structure of an action: The root node of an action has as children the root nodes of its consequences.

An action's tree structure assigns a unique coordinate or address to each node, represented as a sequence of integers:

* The root node has the empty coordinate :math:`\epsilon`.
* If a node has coordinate :math:`c`, the root node of its :math:`i`-th consequence has the coordinate :math:`c \cdot i`.

This hierarchical structure establishes a local **Action Execution Order**. Because the root node of an action triggers its consequences, the root node is said to execute before its consequences.
Within the list of consequences, actions execute according to their order in the list (left-to-right). Mathematically, the execution order :math:`\prec_{act}` of the nodes within an action is simply
the **lexicographical order** of their coordinates. For example, the root :math:`\epsilon` executes before its first child :math:`1`, which executes before its own first child
:math:`1 \cdot 1`, which executes before the root's second child :math:`2`. This corresponds to what is called a **pre-order traversal**.

A node :math:`n_1` is said to be an **ancestor** of node :math:`n_2` (and :math:`n_2` is a **descendant** of :math:`n_1`) if :math:`n_1` is the parent of :math:`n_2`, or if :math:`n_1` is an ancestor of the parent of :math:`n_2`.

An action inherits its type from its root node:

#. A **Create action** has a Create node as the root.
   The consequences are empty.

#. An **Exercise action** has an Exercise node as the root
   and the consequences are the actions triggered immediately by evaluating the choice.
   The Exercise action is the **parent action** of its consequences.

#. A **Fetch action** has a Fetch node as the root.
   The consequences are empty.
   
The terminology on nodes extends to actions via the root node.
For example, the signatories of a Create action are the signatories of the Create node,
and an Exercise action is (non)consuming if and only if its root node is.
Moreover, an Exercise or a Fetch action on a contract is said to **use** the contract.
Finally, a consuming Exercise is said to **consume** (or **archive**) its contract.

Examples
========

An example of a Fetch action appears in the ``Accept`` choice on a DvP proposal contract from the template ``ProposeSimpleDvP``.
The choice body fetches the ``SimpleAsset`` that Bob allocates to the DvP,
which checks that the asset contract is active and brings the contract instance into the computation,
so that the choice implementation can assert that this asset meets the expectation expressed in the proposal contract.
The next diagram shows this Exercise action with the Fetch action as its first consequence.

.. https://lucid.app/lucidchart/556f7b43-565f-4b94-b670-719135a77bec/edit
.. image:: ./images/dvp-propose-accept-action.svg
   :align: center
   :width: 100%
   :alt: The accept action on Alice's ``ProposeSimpleDvP`` exercised by Bob.

A non-consuming Exercise shows up in the combined ``AcceptAndSettle`` choice on the ``ProposeSimpleDvP`` contract:
This choice is non-consuming so that the ``Accept`` choice exercised in the choice body can consume the proposal contract.
As the next diagram shows, non-consuming Exercises yield multiple references to the same input contract #3.
The diagram also shows that fetches have the same effect: input contract #2 is used twice.

.. _da-dvp-propose-accept-and-settle-action:

.. https://lucid.app/lucidchart/fdcc5894-e013-499e-ba85-de16300381a8/edit
.. image:: ./images/dvp-propose-accept-and-settle-action.svg
   :align: center
   :width: 100%
   :alt: The accept-and-settle action on Alice's ``ProposeSimpleDvP`` exercised by Bob.

.. _da-ledger-subaction:
         
Subactions
==========

This example again highlights the hierarchical structure of actions:
The ``AcceptAndSettle`` action contains the corresponding actions for ``Accept`` and ``Settle`` as its consequences.

More generally, since actions trigger consequences recursively, every action generates a family tree of subsequent events.
For an action `act`, its **subactions** are the action itself together with its **proper subactions**, which are defined
recursively as the subactions of its immediate consequences. If an action produces no consequences (a leaf node), it is its own sole subaction,
meaning it has no proper subactions. The proper subactions of an action can therefore be seen as all the downstream events it triggers during execution.

.. math::

   Sub(A) = \{A\} \cup \bigcup_{A' \in C(A)} Sub(A')

where :math:`C(A)` is the ordered list of immediate consequences of :math:`A`.

The subaction relation is visualized below for Bob's ``Settle`` Exercise.
Each borderless box contains an action (via its tree of nodes) and the nesting of these boxes encodes the subaction relation.
In detail, both the blue and purple boxes are proper subactions of Bob's ``Settle`` action shown in grey.
The green box is a proper subaction of the blue and the grey boxes, and the yellow box is a proper subaction of the purple and the grey boxes.

.. https://lucid.app/lucidchart/dbe05602-46b8-4b23-8957-a8e15af912e6/edit
.. image:: ./images/dvp-settle-subactions.svg
   :align: center
   :width: 60%
   :alt: The subactions of Bob exercising the ``Settle` choice on the DvP contract.


.. _transactions:
         
Transactions
************

A **transaction** is a list of actions that are executed atomically.
Those actions are called the **root actions** of the transaction.
That is, for a transaction `tx = (act`:sub:`1`, \ `, …, act`:sub:`n`), every `act`:sub:`i` is a root action.

For example, if Alice and Charlie have made one DvP proposal each for Bob, then Bob may want to both accept simultaneously.
To that end, Bob exercises both ``Accept`` choices in a single transaction with two root actions (blue and purple), as shown next.
Visually, transactions are delimited by the dashed lines on both sides, to distinguish them from actions.
Like for actions, the input contracts on the left are not part of the transaction.

.. https://lucid.app/lucidchart/acb71942-2a11-417c-ae0a-003c8ea2da69/edit
.. image:: ./images/dvp-accept-two.svg
   :align: center
   :width: 100%
   :alt: A transaction with two top-level actions where Bob accepts two DvP proposal, one from Alice and one from Charlie.

For another example, the consequences of an Exercise action are a list of actions and therefore form a transaction
In the example of the ``Settle`` action on Alice's and Bob's ``SimpleDvP``,
the consequences of the ``Settle`` action form the following transaction,
where actions are ordered left-to-right as before.
The transaction consists of two root actions (blue and purple), namely the two ``Transfer`` actions of the two legs of the DvP.

.. https://lucid.app/lucidchart/b8f2c6d1-654b-4658-adc5-77eb59e27d05/edit
.. image:: ./images/dvp-settle-consequences-are-transactions.svg
   :align: center
   :width: 50%
   :alt: The consequences of the ``Settle`` action are a transaction of two actions, namely the two ``Transfer`` legs of the DvP.

This list structure extends the coordinate system and establishes a **Transaction Execution Order**.
By using the list index :math:`i` as the starting coordinate for the :math:`i`-th root action :math:`act_i`, every node in the transaction forest receives a unique address.
For example, the root node of :math:`act_1` has coordinate :math:`1`, and its first consequence has :math:`1 \cdot 1`. The root node of :math:`act_2` has coordinate :math:`2`.
Just as with individual actions, the execution order of all nodes within the transaction is simply the **lexicographical order** of these extended coordinates.
This ensures that the entire tree of :math:`act_1` executes before the tree of :math:`act_2`. Because every node has a strict position in this order, the entire hierarchical forest
of a transaction can be logically **flattened into a single, chronological sequence** of execution.

The hierarchical structure of actions extends to transactions and yields the notion of subtransactions. Conceptually, a subtransaction is a version of the
original execution forest that has been "unrolled" to varying depths across its branches. It is formed by taking a representative sequence of actions
(called a **frontier**) from each root action's tree and concatenating them in their original chronological order.

For every action :math:`A` in a transaction :math:`T = (r_1, \dots, r_n)`, we derive its frontier through a recursive choice:

* **Retain**: Keep the action itself as its own representative sequence: :math:`(A)`.
* **Replace**: Substitute the action with the concatenation of the frontiers of its immediate consequences.
If :math:`A` has no consequences, "replace" results in the empty sequence :math:`()`.

Formally, for an action with consequences :math:`C(A) = (c_1, \dots, c_k)`, the set of all possible **frontiers** :math:`\mathcal{F}(A)` is defined recursively:
.. math::
   \mathcal{F}(A) = \{ (A) \} \cup \left\{ S_1 \oplus \dots \oplus S_k \;\middle|\; S_i \in \mathcal{F}(c_i) \right\}

If :math:`A` is an action with no consequences (a leaf node), the recursion terminates with :math:`\mathcal{F}(A) = \{ (A), () \}`.

The final **subtransaction** is the concatenation of the frontiers chosen for each root action. The set of all possible subtransactions :math:`\mathit{SubTx}(T)`
for the forest is the collection of all such possible combinations:

.. math::

   \mathit{SubTx}(T) = \left\{ S_1 \oplus S_2 \oplus \dots \oplus S_n \;\middle|\; S_i \in \mathcal{F}(r_i) \right\}

This ensures that every part of the original execution is represented (completeness) and no action can appear in a subtransaction alongside its own ancestor or descendant (vertical exclusion).
The result is a flat, ordered sequence that provides a non-overlapping reconstruction of the ledger, tailored to a specific level of detail or privacy.

For example, given the transaction shown above consisting only of the two consequences of the ``Settle`` action,
the next diagram shows all seven proper non-empty subtransactions, each with their dashed delimiters.

.. https://lucid.app/lucidchart/c5ff472e-3161-42a0-ac2d-275774a2b9b8/edit
.. image:: ./images/dvp-settle-consequences-subtransactions.svg
   :align: center
   :width: 100%
   :alt: All proper subtransactions of the consequences of the ``Settle`` action.

The :ref:`privacy model <da-model-privacy>` uses the concept of subtransactions to define the visibility rules.

.. _da-ledger-input-output:
      
Inputs and outputs
******************

The Ledger Model falls into the category of `(extended) UTxO-style ledgers <https://en.wikipedia.org/wiki/Unspent_transaction_output>`_
where the set of unspent transaction outputs (UTxOs) constitutes the current state of a ledger.
Here, the **transaction outputs** are the contract IDs of the contracts created in a transaction.
When a contract is consumed, its contract ID is spent and thus removed from the UTxO set.
The data associated with each UTxO is immutable;
modifications happen by consuming a contract ID and recreating a new contract with a different contract ID.

This Ledger Model extends the UTxO model in two aspects:

* A transaction may use a contract without consuming it, namely, by exercising a non-consuming choice or fetching it.
  In such a case, the contract ID remains in the set of UTxOs even though it appears as an input to a transaction.

* Transactions are structured hierarchically and contract IDs created in the transaction may be consumed within the same transaction.
  For example, inside the ``AcceptAndSettle`` action, the created ``SimpleDvP`` in the first consequence is consumed by the second consequence.
  Such contracts are called **transient**.

These aspects are discussed in more detail in the remaining sections of the Ledger Model.

.. _da-ledger-definition:

Ledger
******

The transaction structure records the contents of a party interaction.
The ledger records two more aspects of an interaction:

* An identifier to uniquely refer a particular party interaction.

* The parties who requested a particular party interaction.

Due to the :ref:`privacy model <da-model-privacy>`, not everyone sees all parts of a party interaction.
A unique identifier for a party interaction allows different parties to correlate whether they see parts of the same interactions.
The notion of an **update** adds such an identifier.
It consists of a single transaction and the so-called **update ID**, a string.
Examples in the Ledger Model use update IDs of the form ``TX i`` for some number ``i``, similar to the transaction view in Daml Studio.
On the Ledger API, update IDs are arbitrary strings whose lexicographic order is independent from their order on the ledger.

A **commit** records the information on *who* requested a party interaction. It bundles an
**update** (the transaction and its unique ID) with the one or more parties that requested it.
These parties are called the **requesters** of the commit.
In Daml Script, the requesters correspond to the ``actAs`` parties given to the ``submit`` commands.

Up to this point, we have focused on the constituents of a party interaction. To build a ledger, it must also be defined how multiple
interactions are arranged relative to one another. When commits are recorded, they are linked together in a graph structure, with the
commits represented as **vertices** and the links between them represented as directed edges. This structure is called a **Directed Acyclic Graph (DAG)**.

.. admonition:: Definition: Directed Acyclic Graph (DAG)

   A **Directed Acyclic Graph (DAG)** is a pair $G = (V, E)$ where:

   * $V$ is a set of **vertices** (nodes).
   * $E \subseteq V \times V$ is a set of **directed edges** (arrows).
   * The graph is **acyclic**, meaning for any vertex $v \in V$,
     there is no non-empty path that starts and ends at $v$.

Using this general structure, we can formally define the Ledger.

.. admonition:: Definition: Ledger

   A **Ledger** is a directed acyclic graph (DAG) where the vertices $V$ are **commits** and every commit has a unique update ID.

.. admonition:: Definition: top-level action

   For a commit, the root actions of its transaction are called the **top-level actions**.
   A top-level action of any ledger commit is also a top-level action of the ledger.

A Canton Ledger thus represents the full history of all actions taken by parties.

Because the graph structure of the ledger includes directed edges, it naturally induces a **happens-before order** on the commits in the ledger.

.. admonition:: Definition: happens-before
    We say that commit `c`:sub:`1` *happens before* `c`:sub:`2` if and only if the ledger contains a non-empty path from `c`:sub:`1` to `c`:sub:`2`,
    or equivalently, the transitive closure of the graph contains an edge from `c`:sub:`1` to `c`:sub:`2`.

At this stage, the edges $E$ are arbitrary. Any arrangement of commits that satisfies the acyclic property of a DAG constitutes a valid mathematical ledger.
However, for a ledger to be useful in practice, these edges must follow the logic of the smart contracts they record.

.. note::
   The :ref:`integrity conditions <da-model-integrity>` on a ledger require that the happens-before order respects the lifecycle of contracts.
   For example, the commit that creates a contract must happen before the commit that spends the contract unless they are the same.
   For the next few sections, we will consider only ledgers that meet these conditions.

Visually, a ledger can be represented as a sequence growing from left to right as time progresses.
Below, dashed vertical lines in purple mark the boundaries of commits,
and each commit is annotated with its requester(s) and the update ID.
Blue arrows link each Exercise and Fetch action to the Create action of the input contract.
These arrows highlight that the ledger forms a **transaction graph** in the sense of a UTXO blockchain.

For example, the following Daml Script encodes the whole workflow of the running DvP example.

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-SCRIPT-BEGIN
   :end-before: SNIPPET-SCRIPT-END

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-ACCEPT_AND_SETTLE-BEGIN
   :end-before: SNIPPET-ACCEPT_AND_SETTLE-END

This workflow gives rise to the ledger shown below with four commits:

* In the first commit, Bank 1 requests the creation of the ``SimpleAsset`` of ``1 EUR`` issued to Alice (contract #1).
  
* In the second commit, Bank 2 requests the creation of the ``SimpleAsset`` of ``1 USD`` issued to Bob (contract #2).

* In the third commit, Alice requests the creation of the ``ProposeSimpleDvP`` (contract #3).

* In the fourth commit, Bob requests to exercise the ``AcceptAndSettle`` choice on the DvP proposal.

.. _da-dvp-ledger:
  
.. https://lucid.app/lucidchart/3ef6e9da-b534-4640-bc19-8fa5c7fb3a71/edit
.. image:: ./images/dvp-ledger.svg
   :align: center
   :alt: The sequence of commits for the whole DvP workflow. First, banks 1 and 2 issue the assets, then Alice proposes the DvP, and finally Bob accepts and settles it.

.. note::
   The integrity constraints do not impose an order between independent commits.
   In this example, there need not be edges among the first three commits ``TX 0``, ``TX 1``, and ``TX 2``,
   so they could be presented in any order.

Because the graph is acyclic, the happens-before relationship forms a **strict partial order** on the commits. It is transitive (if `c`:sub:`1` < `c`:sub:`2` and `c`:sub:`2` < `c`:sub:`3`,
then `c`:sub:`1` < `c`:sub:`3`) and irreflexive (a commit cannot happen before itself). Because it is only a *partial* order, not all commits can be compared. It is possible that neither
`c`:sub:`1` < `c`:sub:`2` nor `c`:sub:`2` < `c`:sub:`1` is true.

It is a well-known property that any partial order can be extended into a **total order** (a linear
sequence), or equivalently, the vertices of any finite Directed Acyclic Graph can be arranged into a
linear sequence. In graph theory, this is achieved via a **topological sort**, which arranges the vertices of a
DAG into a linear sequence whose order preserves the partial order induced by the directed edges, i.e. for every directed edge in E from `c1` to `c2`, `c1`
appears before `c2` in the sequence.

For the next sections on Integrity and Privacy, we assume that the Ledger is totally ordered (unless otherwise specified).
We will return to the more general partial order in the :ref:`causality section <local-ledger>`.

.. admonition:: Definition: Global Execution Order

   By combining the totally ordered sequence of commits with the flattened sequence of each transaction, we establish a **Global Execution Order** for all action nodes on the ledger.
   We say a node :math:`n_1` **executes before** :math:`n_2` if:

   1. They are in the same commit, and :math:`n_1` precedes :math:`n_2` in the **Transaction Execution Order**.
   2. They are in different commits :math:`c_1` and :math:`c_2`, and :math:`c_1` appears before :math:`c_2` in the totally ordered ledger.

   Because every node has a rigorous execution order, the entire ledger can be viewed as a single, chronological timeline of actions (events).


