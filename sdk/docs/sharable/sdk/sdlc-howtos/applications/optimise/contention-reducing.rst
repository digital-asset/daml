.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reduce Contention
#################

Contention is natural and expected when programming within a distributed system like Daml in which every action is asynchronous. It is important to understand the different causes of contention, be able to diagnose the root cause if errors of this type occur, and be able to avoid contention by designing contracts appropriately. 

You can use different techniques to manage contention and to improve performance by increasing throughput and decreasing latency. These techniques include the following:

* Add retry logic.
* Run transactions that have causality in series.
* Bundle or batch business logic to increase business transaction throughput.
* Maximize parallelism with techniques such as sharding, while ensuring no contention between shards.
* Split contracts across natural lines to reduce single, high-contention contracts.
* Avoid write-write and write-read contention on contracts. This type of contention occurs when one requester submits a transaction with a consuming exercise on a contract while another requester submits another exercise or a fetch on the same contract. This type of contention cannot be eliminated entirely, since there will always be some latency between a client submitting a command to a participant and other clients learning of the committed transaction. Here are a few scenarios and specific measures you can take to reduce this type of collision:

  * Shard data. Imagine you want to store a user directory on the ledger. At the core, this is of type ``[(Text, Party)]``, where ``Text`` is a display name and ``Party`` is the associated Party. If you store this entire list on a single contract, any two users wanting to update their display name at the same time will cause a collision. If you instead keep each ``(Text, Party)`` on a separate contract, these write operations become independent from each other.

    A helpful analogy when structuring your data is to envision that a template defines a table, where a contract is a row in that table. Keeping large pieces of data on a contract is like storing big blobs in a database row. If these blobs can change through different actions, you have write conflicts.

  * Use non-consuming choices, where possible, as they do not collide. Non-consuming choices can be used to model events that have occurred, so instead of creating a short-lived contract to hold some data that needs to be referenced, that data could be recorded as a ledger event using a non-consuming choice.

  * Avoid workflows that encourage multiple parties to simultaneously exercise a consuming choice on the same contract. For example, imagine an auction contract containing a field ``highestBid : (Party, Decimal)``. If Alice tries to bid $100 at the same time that Bob tries to bid $90, it does not matter that Alice’s bid is higher. The sequencer rejects the second because it has a write collision with the first transaction. It is better to record the bids in separate Bid contracts, which can be updated independently. Think about how you would structure this data in a relational database to avoid data loss due to race conditions.

  * Think carefully about storing ``ContractIds``. Imagine that you create a sharded user directory according to the first bullet in this list. Each user has a ``User`` contract that stores their display name and party. Now assume that you write a chat application, where each ``Message`` contract refers to the sender by ``ContractId`` User.

    If a user changes the display name, that reference goes stale. You either have to modify all messages that the user ever sent, or you cannot use the sender contract in Daml. 

    Contract keys can be used to make this link inside Daml. If the only place you need to link ``Party`` to ``User`` is in the user interface, it might be best to not store contract references in Daml at all.
