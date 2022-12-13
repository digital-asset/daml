.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

System Architecture FAQ
#######################

What does the Sequencer do?
///////////////////////////

The sequencer nodes, together with their shared sequencer backend (blockchain or databased) and the schema of the sequencer backend (native smart contracts or database schema), provide message delivery between Canton nodes that is guaranteed to be order consistent, delivery consistent and multi-cast.

Multi-cast means that Alice can send a single message to multiple recipients (Carol, Dave, etc.) as one operation.

Delivery consistent means that if Alice sends a message to Carol and Dave, then either the message gets delivered to both recipients, or neither.

Order consistent means that if Alice sends a message to Carol, Dave, and others, and Bob sends a message to Carol, Dave, then Carol and Dave see the messages from Alice and Bob in the same order.

**Further Reading**:

- `Requirements on Sequencer Domain Entity <../canton/architecture/overview.html#sequencer>`__
- `Domain internal components <../canton/architecture/domains/domains.html#domain-internal-components>`__

How does Canton process a transaction?
//////////////////////////////////////

Canton's execution model is that the submitting participant node computes the entire transaction using the Daml interpreter and then decomposes it into views (also known as *projections*) to other participants, and then submits those views as part of a “confirmation request” to the other involved participants and the mediator.
The participants validate the received transaction views by re-computing them with the deterministic Daml interpreter, and then send confirmations to the mediator. As all the participants received the data in the same order, the outcome is deterministic, allowing to pin-point malicious behaviour. The mediator processes the confirmations and sends out an aggregated commit message to all involved participants once sufficient confirmations are received. All messages are sent via the sequencer.

**Further Reading**:

- `Transaction processing in Canton <../canton/architecture/overview.html#transaction-processing-in-canton>`__
- :ref:`execution_model`
- :ref:`da-model-projections`

How does Canton ensure privacy?
///////////////////////////////

Most sequencer backend options have limited privacy features. To provide privacy even against the operator of the sequencers and sequencer backend, Canton encrypts all message payloads sent via the sequencer to be readable only by the intended recipients. That includes the transaction payloads sent as part of confirmation requests.

Canton messages are multi-cast, meaning they can have multiple recipients, and in some cases (e.g. commit requests) have different views for the different recipients. The submitter of a message generates a single-use symmetric View Encryption Key for each view, and encrypts the views using those keys. It then encrypts only a seed for that View Encryption Key using the public half of an asymmetric Participant Encryption Keys that each Canton node publishes.

The View Encryption Keys are kept - encrypted for each receiver - with the message payload itself. A receiving node uses their Participant Encryption key to decrypt the seed of the View Encryption Key for each of the views they are entitled to read, and uses a key derivation function (HKDF to be precise) to recover the View Encryption Key and read the view. 

The supported encryption algorithms for asymmetric encryption (Participant Encryption Keys) and symmetric encryption (View Encryption Keys) are listed in the documentation `here <../canton/usermanual/security.html#cryptographic-key-usage>`__.

**Further Reading:**

- `Encryption Keys <../canton/usermanual/security.html#participant-encryption-key>`__
- `View Encryption Keys <../canton/usermanual/security.html#view-encryption-key>`__
- `Cryptographic Key Usage <../canton/usermanual/security.html#cryptographic-key-usage>`__

.. _where_golden_source:

Where does "the golden source" of Daml Ledger data live in Canton?
//////////////////////////////////////////////////////////////////

The short answer is that Daml Ledger data lives both on the Canton participant nodes and on the sequencer backend, meaning the blockchain or database enabled by the driver. The data is stored in the two places in different ways, but remains fully consistent thanks to Canton's deterministic execution model.

All communication between Canton nodes, including the confirmation requests for transactions and the resulting confirmations and rejections, are stored on the sequencer backend. Since Daml and Canton are built around deterministic execution, you can thus consider that data on the sequencer backend, together with the Participant Encryption Keys, to be a complete copy of the Daml Ledger. 

On the flip side, each Participant node stores its view of the Daml Ledger in an unencrypted format suitable for serving the Ledger API. The set of all participant nodes jointly holds the entire ledger state and history.

**Further Reading:**

- `Transaction processing in Canton <../canton/architecture/overview.html#transaction-processing-in-canton>`__


How is Canton able to recover from data loss?
/////////////////////////////////////////////

As discussed in ":ref:`where_golden_source`", the ledger data lives in two places, once encrypted in the sequencer backend, and once unencrypted spread between participant nodes. As long as you have a complete copy in either place, you can recover and continue operation.

As long as the entire ledger history in the sequencer backend is available, and you hold a participant's Participant Encryption Keys, it is possible to recover the participant from the underlying sequencer backend. So if you use a blockchain as the sequencer backend, and can ensure that that blockchain stays available and uncorrupted, you can always recover from participant data loss.

Should your sequencer backend go down, but all participants are still up and running, you can continue running the system by coordinating all participant nodes to “migrate” active contracts to a new domain, with a new sequencer backend.

Should the sequencer backend no longer have the full ledger history, for example due to a domain switch, or because of deliberate Ledger Pruning, participants can still recover from a combination of the partial sequencer backend and a state snapshot. Such a snapshot can come either from a backup, or from the participants' peers. At the time of writing this process is not fully automated but possible through Canton's repair endpoints.

To be able to get snapshots from peers securely, nodes regularly exchange “commitments” via the underlying sequencer backend. You can think of these as hashes of shared state. If Alice and Bob each run a participant, Alice's participant will regularly communicate a hash of the state it shares with Bob's participant and vice versa. As the state is the same, the hash will be the same. This provides real-time consistency checks, allows participants to detect faulty behaviour in domain compoentns, and also helps recovery in the above scenario. Alice can ask Bob for a snapshot of her data shared with Bob, and check its correctness by comparing it to the commitment she made on the sequencer backend.

**Further Reading:**

- `Repairing Participants <../canton/usermanual/operational_processes.html#repairing-participants>`__
- `Backup and Restore <../canton/usermanual/operational_processes.html#backup-and-restore>`__
- `Ledger Pruning <../canton/usermanual/operational_processes.html#ledger-pruning>`__
