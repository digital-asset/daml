.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Managing Latency and Throughput
###############################

Problem Definition
******************

Latency is a measure of how long a business transaction takes to complete. Throughput measures, on average, the number of business transactions possible per second while taking into account any lower or upper bounds which may point to bottlenecks. Defense against latency and throughput issues can be written into the Daml application during design. 

First we need to identify the potential bottlenecks in a Daml application. We can do this by analyzing the domain-specific transactions.

Each Daml business transaction kicks off when a Ledger API client sends the commands ``create`` or ``exercise`` to a participant. 

.. important::

    * Ledger transactions are not synonymous with business transactions. 
    * Often a complete business transaction spans multiple workflow steps and thus multiple ledger transactions.
    * Multiple business transactions can be processed in a single ledger transaction through batching.
    * Expected ledger transaction latencies are on the order of 0.5-1 seconds on database sequencers, and multiple seconds on blockchain sequencers.

Refer to the `Daml execution model <../intro/7_Composing.html#daml-s-execution-model>`__  that describes a ledger transaction processed by the Canton ledger. The table below highlights potential resource-intensive activities at each step.


.. list-table:: 
   :widths: 10 25 35 30
   :header-rows: 1

   * - Step
     - Participant
     - Resources used
     - Possible bottleneck drivers
   * - Interpretation
     - * Submitting participant node
     -   #. CPU
         #. Memory
         #. DB read access
     -   #. Calculation complexity
         #. Size and number of variables
         #. Number of contract fetches
   * - Blinding
     - * Submitting participant node
     - * CPU/memory
     - * Number and size of views
   * - Submission
     -   * Submitting participant node
         * Sequencer
     -   #. CPU
         #. Memory
     -   #. Serialization/deserialization
         #. Transaction size/number of views
   * - Sequencing
     - * Sequencer
     -   #. Backend storage
         #. Network bandwidth
     -   #. Transaction size
         #. Transaction size/number of views
   * - Validation
     - * Receiving participant nodes
     -   #. Network bandwidth
         #. CPU
         #. Memory
         #. DB read throughput
     -   #. Transaction size -> download, deserialization, storage costs
         #. Computation complexity
         #. Number of contract fetch reads
         #. Number and size of variables
   * - Confirmation
     -   * Validating participant nodes
         * Sequencer
     -   #. Network bandwidth
         #. Sequencer network
         #. Backend write throughput
     -   * Number of confirming parties
   * - Mediation
     -   * Mediator nodes
     -   #. Network throughput
         #. CPU
         #. Memory
     -   * Number of confirming parties
   * - Commit
     -   * Mediator nodes
         * Sequencer
     -   #. CPU
         #. Memory
         #. DB
         #. Network bandwidth
     -   * Number of confirming parties


Possible Throughput Bottlenecks in Order of Likelihood
------------------------------------------------------

#. Transaction size causing high serialization/deserialization and encryption/decryption costs on participant nodes. 
#. Transaction size causing sequencer backend overload, especially on blockchains.
#. High interpretation and validation cost due to calculation complexity or memory use.
#. Large number of involved nodes and associated network bandwidth on sequencer.

Latency can also be affected by the above factors. However, baseline latency usually has more to do with system set-up issues (DB or blockchain latency) rather than Daml modeling problems.

Solutions
---------

#. **Minimize transaction size.** 

Each of the following actions in Daml adds a node to the transaction containing the payload of the contract being acted on. A large number of such operations, and/or operations of this kind on large contracts, are the most common cause of performance bottlenecks. 

    * ``create``
    * ``fetch``
    * ``fetchByKey``
    * ``lookupByKey``
    * ``exercise``
    * ``exerciseByKey``
    * ``archive``

Use the above actions sparingly. For example, if contracts have intermediary states within a transaction, you can often skip them by writing only the end state. For example:

.. code-block:: daml

    template Incrementor
    with
    p : Party
    n : Int
    where
    signatory p
    
    choice Increment : ContractId Incrementor
        controller p
        do create this with n = n+1
    
    -- This adds all m-1 intermediary versions of
    -- the contract to the transaction tree
    choice BadIncrementMany : ContractId Incrementor
        with m : Int
        controller p
        do foldlA (\self' _ -> exercise self' Increment) self [1..m]
    
    -- This only adds the end result to the transaction
    choice GoodIncrementMany : ContractId Incrementor
        with m : Int
        controller p
        do create this with n = n+m

When you need to read a contract, or act on a single contract in multiple ways, you can often bundle those operations into a single action. For example:

.. code-block:: daml

    template Asset
 with
   issuer : Party
   owner : Party
   quantity : Decimal
 where
   signatory [issuer, owner]
 
   -- BadMerge acts on each of the otherCids three times:
   -- Once for validation
   -- Once to extract the quantities
   -- Once to archive
   choice BadMerge : ContractId Asset
     with otherCids : [ContractId Asset]
     controller owner
     do
       -- validate the cids.
       forA_ otherCids (\cid -> do
         other <- fetch cid
         assert (other.issuer == issuer && other.owner == owner))
 
       -- extract the quantities
       quantities <- forA otherCids (\cid -> do
         other <- fetch cid
         return other.quantity)
 
       -- archive the others
       forA_ otherCids archive
 
       create this with quantity = quantity + sum quantities
 
   -- Allow us to do a fetch and an archive in one action
   choice ConsumingFetch : Asset
     controller owner
     do return this
      
   -- GoodMerge only acts on each of the other assets once.
   choice GoodMerge : ContractId Asset
     with otherCids : [ContractId Asset]
     controller owner
     do
       -- Get and archive the others
       others <- forA otherCids (`exercise` ConsumingFetch)
      
       -- validate
       forA_ others (\other -> do
         assert (other.issuer == issuer && other.owner == owner))
 
       -- extract the quantities
       let quantities = map (.quantity) others
 
       create this with quantity = quantity + sum quantities
 

Separate templates for large payloads that change rarely and require minimum access from those for fields that change with almost every action. This optimizes resource consumption for multiple business transactions. 

This batching approach makes updates in one transaction submission rather than requiring separate transactions for each update. Note: this option can cause a small increase in latency and may increase the possibility of command failure but this can be avoided. For example:

.. code-block:: daml

    template T
    with
    p : Party
    where
    signatory p
    
    choice Foo : ()
        controller p
        do return ()
    
    batching : Script ()
    batching = do
    p <- allocateParty "p"
    
    -- without batching we have 10 ledger
    -- transactions.
    cid1 <- submit p do createCmd T with ..
    cid2 <- submit p do createCmd T with ..
    cid3 <- submit p do createCmd T with ..
    cid4 <- submit p do createCmd T with ..
    cid5 <- submit p do createCmd T with ..
    
    submit p do exerciseCmd cid1 Foo
    submit p do exerciseCmd cid2 Foo
    submit p do exerciseCmd cid3 Foo
    submit p do exerciseCmd cid4 Foo
    submit p do exerciseCmd cid5 Foo
    
    -- With batching, there are only two ledger transactions.
    cids <- submit p do
    replicateA 5 $ createCmd T with ..
    submit p do
    forA_ cids (`exerciseCmd` Foo)

2. CPU and memory issues: Use the `Daml profiler <../tools/profiler.html>`__ to analyze Daml code execution. 
3. Once you feel interpretation is not the bottleneck, scale up your machine.

.. tip::

    Profile the JVM and monitor your databases to see where the bottlenecks occur.
