.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _da-ledgers:

Ledger Model
############

.. note::

   * Update examples and notation in the style of the Canton polyglott whitepaper
     https://www.canton.network/hubfs/Canton%20Network%20Files/whitepapers/Polyglot_Canton_Whitepaper_11_02_25.pdf

     Interleave definitions and diagrams with concrete examples from Daml; link to Daml source files that are checked by CI.

   * Remove exceptions

   * Extend with time, upgrading (via packages), requester of a commit, explicit disclosure

   


Canton enables multi-party workflows by providing parties with a *virtual global ledger* (VGL),
which encodes the current state of their shared contracts written in a smart contract language.
At a high level, the interactions are visualized below:
Three parties Alice, Bob, and Charlie connect independently to a virtual global ledger, depicted as a large cloud.
They hold different views of that ledger in their local databases shown as blue icons.
The global ledger is virtual in the sense that no single entity typically sees this global ledger in its entirety;
it is an imaginary database that represents the union of all parties' databases.

.. https://lucid.app/lucidchart/e119dde8-4abe-4b09-9a95-5ca6ef3fb509/edit
.. image:: ./images/da-ledger-model.svg
   :alt: A conceptual diagram of the Virtual Global Ledger.

The ledger is fundamentally a record of interactions between actors resulting in shared facts that are stored as **contracts**.

The actors on the ledger are called **parties**. Parties are the entities that authorize actions,
sign transactions, and hold specific viewing rights. They can represent the people, organizations, or
automated systems participating in a workflow. When parties interact and agree on a set of facts or
obligations, that agreement is recorded on the ledger as a **contract**. A contract holds the actual data or state of the
ledger at any given time. Every contract is assigned a unique, system-generated **contract ID** so the
ledger can track its lifecycle.

A contract is instantiated from a blueprint known as a **template**, which defines the rules of engagement with it.
A template specifies what data the contract must contain, who is required to authorize it, and what subsequent
actions (called **choices**) parties can take on it.

A contract can also optionally be assigned a **contract key**, which is a unique, human-readable identifier tied to
real-world data that allows parties to easily look up and interact with the contract over time.

The Ledger Model defines:

  #. What the changes and the ledgers look like - the :ref:`structure <ledger-structure>` of the Canton Ledger
  #. Who sees which changes and data - the :ref:`privacy model <da-model-privacy>` for the Canton Ledger
  #. What changes to the ledger are allowed and who can request them - the integrity model for the Canton Ledger

The sections below review these concepts of the Ledger Model in turn and how they relate to Daml smart contracts.

.. _da-ledgers-running-example:

Running example
***************

The running example for the Ledger Model is a simple multi-party interaction of two parties atomically swapping their digital assets.
A digital asset is modeled as a ``SimpleAsset`` with an issuer, an owner, and the asset description.
The owner can transfer such an asset to a new owner with the ``Transfer`` choice.

.. note::
   These Daml templates are for illustration purposes only.
   They are not meant for real-world usage as they are heavily simplified.

.. literalinclude:: ./daml/SimpleAsset.daml
   :language: daml
   :start-after: SNIPPET-ASSET-START
   :end-before: SNIPPET-ASSET-END

An atomic swap, also known as delivery versus payment (DvP), combines two asset transfers between the parties in a single transaction.
The ``SimpleDvP`` template below captures the agreement between two parties ``partyA`` and ``partyB`` to swap ownership of the two allocated assets.
Either party to the DvP can execute the swap by exercising the ``Settle`` choice.

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-DVP-BEGIN
   :end-before: SNIPPET-DVP-END

To create a DvP contract instance, the parties go through the usual propose-accept workflow pattern shown next.
Party ``proposer`` creates a proposal for the party ``counterparty`` with their allocated asset and the description of the asset they expect to swap with.
The ``counterparty`` can accept the proposal with the ``Accept`` choice to create a ``SimpleDvP`` contract, or can accept and immediately settle the swap with the ``AcceptAndSettle`` choice.

.. literalinclude:: ./daml/SimpleDvP.daml
   :language: daml
   :start-after: SNIPPET-PROPOSAL-BEGIN
   :end-before: SNIPPET-PROPOSAL-END

.. .. toctree::
:maxdepth: 3
ledger-structure
ledger-privacy
ledger-validity
ledger-integrity
ledger-daml
ledger-exceptions
