.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Anti-patterns
#############

This documents DLT anti-patterns, their drawbacks and more robust ways of achieving the same outcome.

.. contents:: :local:

Don’t use the ledger for orchestration
**************************************

Applications often need to orchestrate calculations at specific times or in a long-running sequence of steps.
Examples are:

* Committing assets to a settlement cycle at 10:00 am
* Starting a netting calculation after trade registration has finished
* Triggering the optimization of a portfolio

At first, creating a contract triggering this request might seem convenient:

.. code-block:: daml

  template OptimizePortfolio
    with
      self: Party
    where
      signatory self

However, this is a case of using a database [ledger] for interprocess communication. This contract is a computational
request from the orchestration unit to a particular program. But the ledger represents the legal rights and obligations
associated with a business process: computational requests are a separate concern and shouldn’t be mixed into this.
Having them on-ledger has the following drawbacks:

* Code bloat in shared models: introduces more things which need to be agreed upon
* Limited ability to send complicated requests since they first have to be projected into smart contracts
* High latency since intermediate variables have to be committed to the ledger
* Changing the orchestration of a production system has a very high barrier since it may require DAML model upgrades
* Orchestration contracts have no business meaning and contaminate the ledger holding business-oriented legal rights and obligations

Instead, lightweight remote procedure calls (RPC) would be more appropriate. A system designer can consider triggering
the application waiting to execute a task with RPC mechanism like:

* An HTTP request
* A general message bus
* A scheduler starting the calculation at a specific time

Notification contracts, which draw a line in the sand and have a real business meaning, don’t fall under this
categorization. These are persistent contracts with real meaning to the business process and not an ephemeral
computational request as described above.

Avoid race conditions in smart contracts
****************************************

The DLT domain lends itself to race conditions. How? Multiple parties are concurrently updating shared
resources (contracts). Here’s an example that’s vulnerable to race conditions: a DvP where a payer allocates their
asset, a receiver has to allocate their cash and then an operator does the final settlement.

.. code-block:: daml

   template DvP
    with
      operator: Party
      payer: Party
      receiver: Party
      assetCid: Optional (ContractId Asset)
      cashIouCid: Optional (ContractId CashIou)
   --

      controller payer can
        PayerAllocate: ContractId DvP
   --

      controller receiver can
        ReceiverAllocate: ContractId DvP
   --

      controller operator can
        Settle: (ContractId Asset, ContractId CashIou)

If the payer and receiver react to the creation of this contract and try to exercise their respective choices, one
will succeed and the other will result in an attempted double-spend. Double-spends create additional work on the
system because when an exception is returned, a new command needs to be subsequently generated and reprocessed. In
addition, the application developer has to implement careful error handling associated with the failed command
submission. It should be everyone's goal to write double-spend free code as needless exceptions dirty logs and
can be a distraction when debugging other problems.

To write your code in a way that avoids race conditions, you should explicitly break up the updating of the state
into a workflow of contracts which collect up information from each participant and is deterministic in execution. For
the above example, deterministic execution can be achieved by refactoring the DvP into three templates:

1. ``DvPRequest`` created by the operator, which only has a choice for the payer to allocate.
2. ``DvP`` which is the result of the previous step and only has a choice for the receiver to allocate.
3. ``SettlementInstruction`` which is the result of the previous step. It has all the information required for settlement and can be advanced by the operator

Alternatively, if asynchronicity is required, the workflow can be broken up as follows:

1. Create a ``PayerAllocation`` contract to collect up the ``asset``.
2. Create a ``ReceiverAllocation`` contract to collect up the ``cashIou``.
3. Have the ``Settle`` choice on the ``DvP`` which takes the previous two contracts as arguments.

Don’t use status variables in smart contracts
*********************************************

When orchestrating the processing of an obligation, the obligation may go through a set of states. The simplest example is locking an asset where the states are locked versus unlocked. A more complex example is the states of insurance claim:

1. Claim Requested
2. Cleared Fraud Detection
3. Approved
4. Sent for Payment

Initially, it might seem that a convenient way to represent this is with a status variable like below:

.. code-block:: daml

  data ObligationStatus = ClaimRequested | ClearedFraudDetection | Approved | SentForPayment deriving (Eq, Show)

  template Obligation
    with
      insuranceUnderwriter: Party
      claimer: Party
      status : ObligationStatus

Instead, you can break up the obligation into separate contracts for each of the different states.

.. code-block:: daml

  template ClaimRequest
    with
      insuranceUnderwriter: Party
      claimer: Party

  template ClaimClearedFraudDetection
    with
      insuranceUnderwriter: Party
      claimer: Party

The drawbacks of maintaining status variables in contracts are:

* It is harder to understand the state of the ledger since you have to inspect contracts
* More complex application code is required since it has to condition on the state the contract
* Within the contract code, having many choices on a contract can make it ambiguous as to how to advance the workflow forward
* The contract code can become complex supporting all the various way to update its internal state
* Information can be leaked to parties who are not involved in the exercising of a choice
* It is harder to update the ledger/models/application if a new state is introduced
* Increased error checking code required to verify the state transitions are correct
* Makes the code harder to reason about

By breaking the contract up and removing the status variable, it eliminates the above drawbacks and makes the system
transparent in its state and how to evolve forward.


