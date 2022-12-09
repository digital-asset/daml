.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Managing Active Contract Set (ACS) Size
#######################################

Problem definition
******************

The Active Contract Set (ACS) size makes up the load related to the number of active contracts in the system at any one time. It means the totality of all the contracts that have been created but not yet archived. ACS size may come from a deliberate Daml workflow design, but the size may also be unexpected when insufficient care is given to supporting and auxiliary contract lifetimes.

.. tip::

    See the documentation on `Daml contracts <../intro/1_Token.html#basic-contracts>`__ for more information.

In Daml systems, ACS size can reach orders of magnitude higher than synonymous loads in common database or blockchain systems. When the ACS size is in the high 100s GBs or TBs, local database access performance may deteriorate. We will look at potential issues around large ACS size and possible solutions.


Relational databases
********************

Large ACS can have a negative impact on many aspects of system performance in relational databases. The following points focus on PostgreSQL as the underlying database; the details differ in the case of Oracle but the results are similar.

* Large ACS size directly affects the resource consumption and performance of a Ledger API client application dealing with a large data set that may not fit into the memory or the application database.
* The size directly affects the speed at which the ACS can be transmitted from the Ledger API server using the ActiveContractService. In extreme cases, it could take hours to transfer the complete set requested by the application due to the limits imposed by the gRPC channel capacity and the speed of storage queries.
* Increased latency is a less direct impact which shows up wherever a query is issued to the database index to make progress. Large ACS size means that the corresponding indices are also rather big, and at a certain point they will no longer fit into the shared-buffer space. It then takes increasingly longer for the database engine to produce query results. This affects activities such as contract lookups during the command submission, transaction tree streaming, or pointwise transaction lookups.
* Large ACS may affect the speed at which the database underpinning the participant ingests new transactions. Normally, as new updates pour in, the write-ahead log commits the table and index changes immediately. Those updates come in two shapes; full-page writes or differential writes. With large volumes, many are full-page writes. 
* Finally, many dirty pages also translate into prolonged and expensive flush to the disk as part of the checkpointing process.


Solutions
---------

* Pay attention to the lifetime of the contracts. Make sure that the supporting and auxiliary contracts don’t clutter the ACS and archive them as soon as it is practical to do so.
* Set up a frequent pruning schedule. Be aware that pruning is only effective if there are archived contracts available for pruning. If all contracts are still active, pruning has limited success. Refer to our `pruning documentation <../../canton/usermanual/pruning.html>`__ for more information.
* Implement an ODS in your ledger client application to limit the reliance on read access to the ACS. Do this whenever you notice that the time to initialize the application from the ACS exceeds your pain level.
* Monitor database performance. 
    * Monitor the disk read and disk write activity. Look for sudden changes in the operation patterns. For instance, a sudden uplift in the disk’s read activity may be a sign of indices no longer fitting into the shared buffers.
    * Observe the performance of the database queries. Check our monitoring documentation for `query metrics <../../canton/usermanual/monitoring.html#daml-index-db-operation-query>`__ that can assist. You may also consider setting up a `log_min_duration_statement parameter <https://www.postgresql.org/docs/current/runtime-config-logging.html>`__ in the PostgreSQL configuration.
* Set up `autovacuum <https://www.postgresql.org/docs/13/routine-vacuuming.html>`__ on the PostgreSQL database. Note that, after pruning, a lot of dead tuples will need removing.

HTTP JSON API Service
*********************

We recommend using a relational database and dedicated compute resources to manage large ACS size when using the HTTP JSON API and refer the reader to the above considerations. 

.. tip::

    See the HTTP JSON API service documentation on `managing high load in the query store <../../json-api/production-setup/query-store.html#behavior-under-high-load>`__ and `server scaling and redundancy <../../json-api/production-setup/scaling-and-redundancy.html>`__ for more information.