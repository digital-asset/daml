.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Example Application with Techniques for Reducing Contention
###########################################################

The example application below illustrates the relationship between blockchain and business application performance, as well as the impact of design choices. Trading, settlement, and related systems are core use cases of blockchain technology, so this example demonstrates different ways of designing such a system within a UTXO ledger model and how the design choices affect application performance.

The Example Minimal Settlement System
*************************************

This section defines the requirements that the example application should fulfill, as well as how to measure its performance and where contention might occur. Assume that there are initial processes already in place to issue assets to parties. All of the concrete numbers in the example are realistic order-of-magnitude figures that are for illustrative purposes only. 

Basic functional requirements for the example application
=========================================================

A trading system is a system that allows parties to swap assets. In this example, the parties are Alice and Bob, and the assets are shares and dollars. The basic settlement workflow could be:

#. **Proposal**: Alice offers Bob to swap one share for $1.
#. **Acceptance**: Bob agrees to the swap.
#. **Settlement**: The swap is settled atomically, meaning that at the same time Alice transfers $1 to Bob, Bob transfers one share to Alice.

Practical and security requirements for the example application
===============================================================

The following list adds some practical matters to complete the rough functional requirements of an example minimal trading system.

* Parties can hold *asset positions* of different asset types which they control.

  * An asset position consists of the type, owner, and quantity of the asset.
  * An asset type is usually the combination of an on-ledger issuer and a symbol (such as currency, CUSIP, or ISIN).

* Parties can transfer an asset position (or part of a position) to another party.
* Parties can agree on a settlement consisting of a swap of one position for another.
* Settlement happens atomically.
* There are no double spends.
* It is possible to constrain the *total asset position* of an owner to be non-negative. In other words, it is possible to ensure that settlements are funded. The total asset position is the sum of the quantities of all assets of a given type by that owner.

Performance measurement in the example application
==================================================

Performance in the example can be measured by latency and throughput; specifically, settlement latency and settlement throughput. Another important factor in measuring performance is the ledger transaction latency. 

* **Settlement latency**: the time it takes from one party wanting to settle (just before the proposal step) to the time that party receives final confirmation that the settlement was committed (after the settlement step). For this example, assume that the best possible path occurs and that parties take zero time to make decisions.
* **Settlement throughput**: the maximum number of settlements per second that the system as a whole can process over a long period.
* **Transaction latency**: the time it takes from when a client application submits a command or transaction to the ledger to the time it receives the commit confirmation. The length of time depends on the command. A transaction settling a batch of 100 settlements will take longer than a transaction settling a single swap. For this example, assume that transaction latency has a simple formula of a fixed cost ``fixed_tx`` and a variable processing cost of ``var_tx`` times the number of settlements, as shown here:

  ``transaction latency = fixed_tx + (var_tx * #settlements)``

* Note that the example application does not assign any latency cost to settlement proposals and acceptances.
* For the example application, assume that:

  * ``fixed_tx = 250ms``
  * ``var_tx = 10ms``

To set a baseline performance measure for the example application, consider the simplest possible settlement workflow, consisting of one proposal transaction plus one settlement transaction done back-to-back. The following formula approximates the settlement latency of the simple workflow:

  ``(2 * fixed_tx) + var_tx``

``=`` ``(2 * 250ms) + 10ms``

``=`` ``510ms``

To find out how many settlements per second are possible if you perform them in series, throughput evaluates to the following formula (there are 1,000ms in one second):

  ``1000ms / (fixed_tx + var_tx) settlements per second``

``=`` ``1000ms / (250ms + 10ms)``

``=`` ``1000 / 260``

``=`` ``3.85 or ≈ 4 settlements per second``

These calculations set the optimal baselines for a high performance system.

The next goal is to increase throughput without dramatically increasing latency. Assume that the underlying DLT has limits on total throughput and on transaction size. Use a simple cost model in a unit called ``dlt_min_tx`` representing the minimum throughput unit in the DLT system. An empty transaction has a fixed cost ``dlt_fixed_tx`` which is:

  ``dlt_fixed_tx = 1 dlt_min_tx``

Assume that the ratio of the marginal throughput cost of a settlement to the throughput cost of a transaction is roughly the same as the ratio of marginal latency to transaction latency (shown previously). A marginal settlement throughput cost ``dlt_var_tx`` can then be determined by this calculation:

  ``dlt_var_tx = ratio * dlt_fixed_tx``

``=`` ``dlt_var_tx = (var_tx / fixed_tx) * dlt_fixed_tx``

``=`` ``dlt_var_tx = 10sm/250ms * dlt_fixed_tx``

``=`` ``dlt_var_tx = 0.04 * dlt_fixed_tx``

and, since from previously

  ``dlt_fixed_tx = 1 dlt_min_tx``

then

  ``dlt_var_tx = 0.04 * dlt_min_tx``

Even with good parallelism, ledgers have limitations. The limitations might involve CPUs, databases, or networks. Calculate and design for whatever ceiling you hit first. Specifically, there is a maximum throughput ``max_throughput`` (measured in ``dlt_min_tx/second``) and a maximum transaction size ``max_transaction`` (measured in ``dlt_min_tx``). For this example, assume that ``max_throughput`` is limited by being CPU-bound. Assume that there are 10 CPUs available and that an empty transaction takes 10ms of CPU time. For each second: 

  ``max_throughput = 10 * each CPU’s capacity``

Each ``dlt_min_tx`` takes 10ms and there are 1,000 ms in a second. The capacity for each CPU is then 100 ``dlt_min_tx`` per second. The throughput calculation becomes:

  ``max_throughput = 10 * 100 dlt_min_tx/second``

``=`` ``max_throughput = 1,000 dlt_min_tx/second``

Similarly, ``max_transaction`` could be limited by message size limit. For this example, assume that the message size limit is 3 MB and that an empty transaction ``dlt_min_tx`` is 1 MB.  So

  ``max_transaction = 3 * dlt_min_tx``

One of the three transactions needs to hold an approval with no settlements. That leaves the equivalent of ``(2 * dlt_min_tx)``  available to hold many settlements in the biggest possible transaction. Using the ratio described earlier, each marginal settlement ``dlt_var_tx`` takes ``0.04 * dlt_min_tx``. So the maximum number of settlements per second is:

  ``(2 * dlt_min_tx)/(0.04 * dlt_min_tx)``

``=`` ``50 settlements/second``

Using the same assumptions, if you process settlements in parallel rather than in series (with only one settlement per transaction), latency stays constant while settlement throughput increases. Earlier, it was noted that a simple workflow can be ``(2 * fixed_tx) + var_tx``. In the DLT system, the simple workflow calculation is:

  ``(2 * dlt_min_tx) + dlt_var_tx``

``=`` ``(2 * dlt_min_tx) + (0.04 * dlt_min_tx)``

``=`` ``2.04 * dlt_min_tx``

It was assumed earlier that max_throughput is ``1,000 dlt_min_tx/second``. So the maximum number of settlements per second possible through parallel processing alone in the example DLT system is:

  ``1,000/2.04 settlements per second``

``=`` ``490.196 or ~490 settlements per second``

These calculations provide a baseline when comparing various techniques that can improve performance. The techniques are described in the following sections.

Prepare Transactions for Contention-Free Parallelism
****************************************************

This section examines which aspects of UTXO ledger models can be processed in parallel to improve performance. In UTXO ledger models, the state of the system consists of a set of immutable contracts, sometimes also called UTXOs.

Only two things can happen to a contract: it is created and later it is consumed (or spent). Each transaction is a set of input contracts and a set of output contracts, which may overlap. The transaction creates any output contracts that are not also consumed in the same transaction. It also consumes any input contracts, unless they are defined as non-consumed in the smart contract logic.

Other than smart contract logic, the execution model is the same for all UTXO ledger systems:

#. **Interpretation**: the submitting party precalculates the transaction, which consists of input and output contracts.
#. **Submission**: the submitting party submits the transaction to the network.
#. **Sequencing**: the consensus algorithm for the network assigns the transaction a place in the total order of all transactions.
#. **Validation**: the transaction is validated and considered valid if none of the inputs were already spent by a previous transaction.
#. **Commitment**: the transaction is committed.
#. **Response**: the submitting party receives a response that the transaction was committed.

The only step in this process which has a sequential component is sequencing. All other stages of transaction processing are parallelizable, which makes UTXO a good model for high-performance systems. However, the submitting party has a challenge. The interpretation step relies on knowing possible input contracts, which are by definition unspent outputs from a previous transaction. Those outputs only become known in the response step, after a minimum delay of ``fixed_tx``.

For example, if a party has a single $1,000 contract and wants to perform 1,000 settlements of $1 each, sequencing in parallel for all 1,000 settlements leads to 1,000 transactions, each trying to consume the same contract. Only one succeeds, and all the others fail due to contention. The system could retry the remaining 999 settlements, then the remaining 998, and so on, but this does not lead to a performant system. On the other hand, using the example latency of 260ms per settlement, processing these in series would take 260s or four minutes 20s, instead of the theoretical optimum of one second given by ``max_throughput``. The trading party needs a better strategy. Assume that:

  ``max_transaction > dlt_fixed_tx + 1,000 * dlt_var_tx = 41 dlt_min_tx``

The trading party could perform all 1,000 settlements in a single transaction that takes:

  ``fixed_tx + 1,000 * var_tx = 10.25s``

If the latency limit is too small or this latency is unacceptable, the trading party could perform three steps to split $1,000 into:

* 10 * $100
* 100 * $10
* 1,000 * $1

and perform the 1,000 settlements in parallel. Latency would then be theoretically around: 

  ``3 * fixed_tx + (fixed_tx + var_tx) = 1.01s``

However, since the actual settlement starts after 750 ms, and the ``max_throughput`` is ``1,000 dlt_min_tx/s``, it would actually be:

  ``0.75s + (1,000 * (dlt_fixed_tx + dlt_var_tx)) / 1,000 dlt_min_tx/s = 1.79s``

These strategies apply to one particular situation with a very static starting state. In a real-world high performance system, your strategy needs to perform with these assumptions:

* There are constant incoming settlement requests, which you have limited ability to predict. Treat this as an infinite stream of random settlements from some distribution and maximize settlement throughput with reasonable latency.
* Not all settlements are successful, due to withdrawals, rejections, and business errors.

To compare between different techniques, assume that the settlement workflow consists of the steps previously illustrated with Alice and Bob:

#. **Proposal**: proposal of the settlement
#. **Acceptance**: acceptance of the settlement
#. **Settlement**: actual settlement

These steps are usually split across two transactions by bundling the acceptance and settlement steps into one transaction. Assume that the first two steps, proposal and acceptance, are contention-free and that all contention is on settlement in the last step. Note that the cost model allocates the entire latency and throughput costs ``var_tx`` and ``dlt_var_tx`` to the settlement, so rather than discussing performant trading systems, the concern is for performant settlement systems. The following sections describe some strategies for trading under these assumptions and their tradeoffs.

Non-UTXO Alternative Ledger Models
***********************************

As an alternative to a UTXO ledger model, you could use a replicated state machine ledger model, where the calculation of the transaction only happens after the sequencing.

The steps would be:

#. **Submission**: the submitting party submits a command to the network.
#. **Sequencing**: the consensus algorithm of the network assigns the command a place in the total order of all commands.
#. **Validation**: the command is evaluated to a transaction and then validated.
#. **Response**: the submitting party receives a response about the effect of the command.

**Pros**

This technique has a major advantage for the submitting party: no contention. The party pipes the stream of incoming transactions into a stream of commands to the ledger, and the ledger takes care of the rest.

**Cons**

The disadvantage of this approach is that the submitting party cannot predict the effect of the command. This makes systems vulnerable to attacks such as frontrunning and reordering.

In addition, the validation step is difficult to optimize. Command evaluation may still depend on the effects of previous commands, so it is usually done in a single-threaded manner. Transaction evaluation is at least as expensive as transaction validation. Simplifying and assuming that ``var_tx`` is mostly due to evaluation and validation cost, a single-threaded system would be limited to ``1s / var_tx = 100`` settlements per second. It could not be scaled further by adding more hardware.

Simple Strategies for UTXO Ledger Models
****************************************

To attain high throughput and scalability, UTXO is the best option for a ledger model. However, you need strategies to reduce contention so that you can parallelize settlement processing. 

Batch transactions sequentially
===============================

Since ``(var_tx << fixed_tx)``, processing two settlements in one transaction is much cheaper than processing them in two transactions. One strategy is to batch transactions and submit one batch at a time in series. 

**Pros**

This technique completely removes contention, just as the replicated state machine model does. It is not susceptible to reordering or frontrunning attacks.

**Cons**

As in the replicated state machine technique, each batch is run in a single-threaded manner. However, on top of the evaluation time, there is transaction latency. Assuming a batch size of ``N < max_settlements``, the latency is:

  ``fixed_tx + N * var_tx``

and transaction throughput is:

  ``N / (fixed_tx + N * var_tx)``

As ``N`` goes up, this tends toward ``1 / var_tx = 100``, which is the same as the throughput of replicated state machine ledgers.

In addition, there is the ``max_settlements`` ceiling. Assuming ``max_settlements = 50``, you are limited to a throughput of ``50 / 0.75 = 67`` settlement transactions per second, with a latency of 750ms. Assuming that the proposal and acceptance steps add another transaction before settlement, the settlement throughput is 67 settlements per second, with a settlement latency of one second. This is better than the original four settlements per second, but far from the 490 settlements per second that is achievable with full parallelism.

Additionally, the success or failure of a whole batch of transactions is tied together. If one transaction fails in any way, all will fail, and the error handling is complex. This can be somewhat mitigated by using features such as Daml exception handling, but contention errors cannot be handled. As long as there is more than one party acting on the system and contention is possible between parties (which is usually the case), batches may fail. The larger the batch is, the more likely it is to fail, and the more costly the failure is.

Use sequential processing or batching per asset type and owner
==============================================================

In this technique, assume that all contention is within the asset allocation steps. Imagine that there is a single contract on the ledger that takes care of all bookkeeping, as shown in this Daml code snippet:

.. code-block:: daml

    template AllAssets
      with
        -- A map from owner and type to quantity
        holdings : Map Party (Map AssetType Decimal)
      where
        signatory (keys holdings)

This is a typical pattern in replicated state machine ledgers, where contention does not matter. On a UTXO ledger, however, this pattern means that any two operations on assets experience contention. With this representation of assets, you cannot do better than sequential batching. There are many additional issues with this approach, including privacy and contract size.

Since you typically only need to touch one owner’s asset of one type at a time and constraints such as non-negativity are also at that level, assets are usually represented by asset positions in UTXO ledgers, as shown in this Daml code snippet:

.. code-block:: daml

    template
      with
        assetType : AssetType
        owner : Party
        quantity : Decimal
      where
        signatory assetType.issuer, owner

An asset position is a contract containing a triple (owner, asset type, and quantity). The total asset position of an asset type for an owner is the sum of the quantities for all asset positions with that owner and asset type. If the settlement transaction touches two total asset positions for the buy-side and two total asset positions for the sell-side, batching by asset type and owner does not help much. 

Imagine that Alice wants to settle USD for EUR with Bob, Bob wants to settle EUR for GBP with Carol, and Carol wants to settle GBP for USD with Alice. The three settlement transactions all experience contention, so you cannot do better than sequential batching.

However, if you could ensure that each transaction only touches one total asset position, you could then apply sequential processing or batching per total asset position. This is always possible to do by decomposing the settlement step into the following:

#. **Buy-side allocation**: the buy-side splits out an asset position from their total asset position and allocates it to the settlement.
#. **Sell-side allocation**: the sell-side splits out an asset position from their total asset position and allocates it to the settlement.
#. **Settlement**: the asset positions change ownership.
#. **Buy-side merge**: the buy-side merges their new position back into the total asset position.
#. **Sell-side merge**: the sell-side merges their new position back into the total asset position.

This does not need to result in five transactions. 

* Buy-side allocation is usually done as part of a settlement proposal. 
* Sell-side allocation is typically handled as part of the settlement. 
* Buy-side merge and sell-side merge technically do not need any action. By definition of total asset positions, merging is an optional step. It is easy to keep things organized without extra transactions. Every time a total asset position is touched as part of buy-side allocation or sell-side allocation above, you merge all positions into a single one. As long as there is a similar amount of inbound and outbound traffic on the total asset position, the number of individual positions stays low.

**Pros**

Assuming that a settlement is considered complete after the settlement step and that you bundle the allocation steps above into the proposal and settlement steps, the system performance will stay at the optimum settlement latency of 510ms. 

Also, if there are enough open settlements on distinct total asset positions, the total throughput may reach up to the optimal 490 settlements per second.

With batch sizes of ``N=50`` for both proposals and settlements and sufficient total asset positions with open settlements, the maximum theoretical settlement throughput is: 

``50 stls * 1,000 dlt_min_tx/s / (2 * dlt_fixed_tx + 50 * dlt_var_tx) = 12,500 stls/s``

**Cons**

Without batching, you are limited to the original four outgoing settlements per second, per total asset position. If there are high-traffic assets, such as the USD position of a central counterparty, this can bottleneck the system as a whole.

Using higher batch sizes, you have the same tradeoffs as for sequential batching, except that it is at a total asset position level rather than a global level. Latency also scales exactly as it does for sequential batching.

Using a batch size of 50, you would get settlement latencies of around 1.5s and a maximum throughput per total asset position of 67 settlements per second, per total asset position.

Another disadvantage is that allocating the buy-side asset in a transaction before the settlement means that asset positions can be locked up for short periods. 

Additionally, if the settlement fails, the already allocated asset needs to be merged back into the total asset position.

Shard Asset Positions for UTXO Ledger Models
********************************************

In systems where peak loads on a single total asset position is in the tens or hundreds of settlements per second, more sophisticated strategies are needed. The total asset positions in question cannot be made up of a single asset position. They need to be sharded.

Shard total asset positions without global constraints
======================================================

Consider a total asset position that represents a bookkeeping position without any on-ledger constraints. For example, the trading system may deal with fiat settlement off-ledger, and you simply want to record a balance, whether it is positive or negative. In this situation, you can easily get rid of contention altogether by assigning all allocations an arbitrary amount. To allocate $1 to a settlement, write two new asset positions of $1 and -$1 to the ledger, then use the $1 to allocate. The total asset position is unchanged.

**Pros**

This approach removes all contention on a total asset position.

Trading between two such total asset positions without global constraints can run at the theoretically optimal latency and throughput. Combining this with batching of batch size 50, it is possible to achieve settlements per second up to the same 12,500 settlements per second per total asset position that are possible globally.

**Cons**

Besides the inability to enforce any global constraints on the total asset position, this creates many new contracts. At 500 settlements per second, two allocations per settlement, and two new assets per allocation, that results in 2,000 new asset positions per second, which adds up quickly.

This effect has to be mitigated by a netting automation that nets them up into a single position once a period (for example, every time it sees >= 100 asset positions for a total position). This automation does not contend with the trading, but it adds up to 20 large transactions per second to the system and slightly reduces total throughput.

Shard total asset positions with global constraints
===================================================

As an example of a global constraint, assume that the total asset position has to stay positive. This is usually done by ensuring that each individual asset position is positive. If that is the case, the strategy is to define a sharding scheme where the total position is decomposed into ``N`` smaller shard positions and then run sequential processing or batching per shard position.

Each asset position has to be clearly assignable to a single shard position so that there is no contention between shards. The partitioning of the total asset position does not have to be done on-ledger. If the automation for all shards can communicate off-ledger, it is possible to run a sharding strategy where you simply set the total number of desired asset positions. 

For example, assume that there should be 100 asset positions for a total asset position with some minimal value. 

* The automation keeps track of a synchronized pending set of asset positions, which marks asset positions that are in use. 
* Every time the automation triggers (which may happen concurrently), it looks at how many asset positions there are relative to the desired 100 and how much quantity is needed to allocate the open settlements. 
* It then selects an appropriate set of non-pending asset positions so that it can allocate the open settlements and return new asset positions to move the total number closer to 100. 
* Before sending the transaction, it adds those positions to the pending set to make sure that another thread does not also use them.

Alternatively, if you have a sufficiently large total position compared to settlement values, you can pick the 99th percentile ``p_99`` of settlement values and maintain ``N-1`` positions of value between ``p_99`` and ``2 * p_99`` and one of the (still large) remainder. 99% of transactions will be processed in the ``N-1`` shard positions, and the remaining 1% will be processed against the remaining pool. Whenever a shard moves out of the desired range, it is balanced against the pool.

**Pros**

Assuming that there is always enough liquidity in the total asset position, the performance can be the same as without global constraints: up to 12,500 settlements per second on a single total asset position.

**Cons**

If settlement values are large compared to total asset holdings, this technique helps little. In an extreme case, if every settlement needs more than 50% of the total holding, it does not perform any better than the sequential processing or batching per asset type and owner technique. 

In realistic scenarios where settlement values are distributed on a broad range relative to total asset position and those relativities change as holdings go up and down, developing strategies that perform optimally is complex. There are competing priorities that need to be balanced carefully:

* Keeping the total number of asset positions limited so that the number of active contracts does not impact system performance.
* Having sufficient large asset positions so that frequent small settlements can be processed in parallel.
* Having a mechanism that ensures large settlements, possibly requiring as much as 100% of the available total asset position, are not blocked.
