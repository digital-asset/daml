.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Avoid Contention Issues
#######################

Measuring the performance of business applications involves more than considering the transactions per second and transaction latency of the underlying blockchain and Distributed Ledger Technology (DLT). Blockchains are distributed systems; even the highest-performance blockchains have considerably higher transaction latencies than traditional databases. These factors make the systems prone to contention, which can stifle the performance of applications when not handled appropriately.

It is, unfortunately, easy to design low-performance applications even on a high-performance blockchain system. Applications that initially perform well may fail under pressure. It is better to plan around contention in your application design than to fix issues later. The marginal cost of including extra business logic within a blockchain transaction is often small.

Contention is expected in distributed systems. The aim is to reduce it to acceptable levels and handle it gracefully, not to eliminate it at all costs. If contention only occurs rarely, it may be cheaper for both performance and complexity to simply let the occasional allocation fail and retry, rather than implement an advanced technique to avoid it.

As an added benefit to reducing contention issues, carefully bundling or batching strategic business logic can improve performance by yielding business transaction throughput that far exceeds the blockchain transaction throughput. 

Contention in Daml
******************

Daml uses an unspent transaction output (UTXO) ledger model. UTXO models enable higher performance by supporting parallel transactions. This means that you can send new transactions while other transactions are still processing. The downside is that contention can occur if a second transaction arrives while a conflicting earlier transaction is still pending. 

Daml guarantees that there can only be one consuming choice exercised per contract. If you try to commit two transactions that would consume the same contract, you have write-write contention.

Contention can also result from incomplete or stale knowledge. For example, a contract may have been archived, but a client hasn’t yet been notified due to latencies or a privacy model might prevent the client from ever knowing. If you try to commit two transactions on the same contract where one transaction reads and the other one consumes an input, you run the risk of a read-write contention. 

A contract is considered pending when you do not know if the output has been consumed. It is best to assume that your transactions will go through and to treat pending ones as probably consumed. You must also assume that acting on a pending contract will fail. 

You need to wait while the sequencer is processing a transaction in order to confirm that an input was consumed from a consuming input request. If you do not get confirmation back from the first transaction before submitting a second transaction on the same contract, the sequence is not guaranteed. The only way to avoid this conflict is to control the sequence of those two transactions.

Ledger state is read in the following places within the `Daml Execution Model <../intro/7_Composing.html#daml-s-execution-model>`__ :

#. A client submits a command based on the client’s latest view of the state of the shared ledger. The command might include references to ``ContractIds`` that the client believes are active.
#. During interpretation, ledger state is used to look up active contracts.
#. During validation, ledger state is again used to look up contracts and to validate the transaction by reinterpreting it.

Contention can occur both between #1 and #2 and between #2 and #3:

* The client is constructing the command in #1 based on contracts it believes to be active. But by the time the participant performs interpretation in #2, it has processed the commit of another transaction that consumed those contracts. The participant node rejects the command due to contention.
* The participant successfully constructs a transaction in #2 based on contracts it believes to be active. But by the time validation happens in #3, another transaction that consumes the same contracts has already been sequenced. The validating participants reject the command due to contention. 

The complete and relevant ledger state at the time of the transaction is known only after sequencing, which happens between #2 and #3.  That ledger state takes precedence to ensure double spend protection.

Contention slows performance significantly. While you cannot avoid contention completely, you can design logic to minimize it. The same considerations apply to any UTXO ledger.
