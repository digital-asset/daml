.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Query Store
###########

.. note:: Daml Open Source only supports PostgreSQL backends for the *HTTP JSON API* server, but Daml Enterprise also supports Oracle backends.

The *HTTP JSON API* server is a JVM application that uses an in-memory backend by default.
This in-memory backend setup is inefficient for larger datasets as every query fetches the entire active contract set for all the templates the query references.
For production setups we therefore recommend, at a minimum, that one use a database as a query store.
This allows for more efficient data caching and improves query performance.
Details for enabling a query store are given below.

The query store is a cached search index and is useful in cases
where the application needs to query large active contract sets (ACS). The *HTTP JSON API* server can be
configured with PostgreSQL/Oracle (Daml Enterprise only) as the query store backend.

The query store is built by saving the state of the ACS up to the current ledger
offset. This allows the *HTTP JSON API* to only request the delta on subsequent queries,
making it much faster than requesting the entire ACS every time.

Configuring
***********

For example, to enable the PostgreSQL backend you can add the ``query-store`` config block in your application config file:

.. code-block:: none

    query-store {
      base-config {
        user = "postgres"
        password = "password"
        driver = "org.postgresql.Driver"
        url = "jdbc:postgresql://localhost:5432/test?&ssl=true"

        // prefix for table names to avoid collisions, empty by default
        table-prefix = "foo"

        // max pool size for the database connection pool
        pool-size = 12
        //specifies the min idle connections for database connection pool.
        min-idle = 4
        //specifies the idle timeout for the database connection pool.
        idle-timeout = 12s
        //specifies the connection timeout for database connection pool.
        connection-timeout = 90s
      }
      // option setting how the schema should be handled.
      // Valid options are start-only, create-only, create-if-needed-and-start and create-and-start
      start-mode = "start-only"
    }

Consult your database vendor's JDBC driver documentation to learn how to specify a JDBC connection URL that suits your needs.

You can also use the ``--query-store-jdbc-config`` CLI flag (deprecated), as shown below.

.. code-block:: shell

    daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 \
    --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,start-mode=start-only"


Managing DB permissions with ``start-mode``
*******************************************

The ``start-mode`` is a custom parameter to specify the initialization and usage of the database backing the query store.

Depending on how you prefer to operate it, you can

* run with ``start-mode=create-only`` with a user
  that has exclusive table-creating rights that are required for the query store
  to operate, and then start it once more with ``start-mode=start-only`` with a user
  that can use the aforementioned tables, but that cannot apply schema changes
* run with a user that can both create and use
  the query store tables by passing ``start-mode=create-and-start``
* run with a user that can drop, create and use
  the query store tables by passing ``start-mode=create-if-needed-and-start``

When restarting the *HTTP JSON API* server after a schema has already been
created, it's safe practice to always use ``start-mode=start-only``.


Data Continuity
***************

The query store is a cache. This means that it is perfectly fine to drop it, as
the data it contains is a subset of what can safely be recovered from the ledger.

As such, the query store does not provide data continuity guarantees across versions
and furthermore doesn't guarantee that a query store initialized with a previous
version of the *HTTP JSON API* will work with a newer version.
However, the query store keeps track of the schema version under which it was initialized and
JSON API refuses to start if an old schema is detected when it's run with a newer version.

To evolve, the operator of the *HTTP JSON API* query store needs to drop the database
used to hold the *HTTP JSON API* query store, create a new one (consult your database
vendor's documentation for instructions), and then (depending on the operator's preferred production setup) should proceed to create and
start the server using either ``start-mode=create-only`` & ``start-mode=start-only``
or only with ``start-mode=create-and-start`` as described above.


Behavior Under High Load
************************

As stated :doc:`in the overview <../json-api>`, the JSON API is optimized for rapid application development and ease of developer onboarding.
It is not intended to support every high-performance use case.
To understand how a high-load application may reach the limits of its design, you need to consider how the query store works.

First, always keep in mind that *the JSON API can only do whatever an ordinary ledger API client application could do, including your own*.
That's because it *is* an ordinary client of :doc:`/app-dev/ledger-api`.
So, if your application's queries are a poor match for the way JSON API's query store works, it's time to consider cutting out the middleman.


Running a Query
===============

Here is what happens every time you run a query with a configured query store:

1. The query store uses the transaction stream from the gRPC API to update its contract table with an up-to-date "view" of all active contracts that match the template IDs, interface IDs, and user party set in the request.
   The payload query is not considered at all; every matching contract is added to the table.
   This will use the active contract service to "skip past" most of the transaction stream, if the contract table is empty at that set.
2. A database query is run on the contract table, filtering on template ID/interface ID, party set, and the payload.
3. If contention with concurrent requests is detected, the query store will assume it is "behind" and "catch up" by returning to #1.
   This uses an iterative "livelocking" strategy, where progress is guaranteed and more concurrency is permitted, rather than exclusive locking.
4. Results are returned to the user.

A websocket query does the same, but any contract that didn't exist at the *start* of the websocket won't receive the above treatment; the "live" data described for the websocket query stream is always filtered directly from the gRPC API, just as if no query store was configured.


Storage Overview
================

Without going into too much detail, here's more or less what is stored under step #1 above, *for each contract*:

1. full contract ID
2. an integer for the template or interface ID
3. for a template ID, the create arguments, as full JSON
4. for an interface ID, the interface view, as full JSON
5. a list of signatories and observers, i.e. parties

Every query store backend indexes on #2, as we have found this index to be universally beneficial.
In addition, the Oracle backend has an index on #3 and #4.

With this indexing arrangement, our testing has indicated reasonable performance for well-matched use cases as explained below for contract tables of up to 100000 contracts.


Well-Matched Use Cases
======================

The query store is, generally speaking, best matched to "CRUD-like" use cases with relatively stable active contract sets.
Here are some more specific characteristics likely to be shared by Daml designs that will perform well with the query store.

1. Workflows properly separated into separate templates.
   The template ID index is the most efficient part of query store filtering.
   In addition, contract table updates on separate template IDs do not contend (i.e. cause the reset to step #1 above), so changes to the ledger on other parts of the workflow do not affect queries on the template in question.
2. Queries that return <10% of all active contracts for a given contract type ID and party set.
   This maximizes the value of storing redundant copies in SQL-queryable form at all, namely, that the JSON API does not even need to consider already-stored, unmatched contracts.
3. Queries against a slow participant.
   If the transaction stream from your ledger API participant server is particularly slow, it may be faster to retrieve most contracts from its local database, even if JSON API gets no benefit from #2.
4. Templates with low churn, i.e. most active contracts from the previous query are likely to still be active for the next query.
   If the query store is likelier to have already stored most of the contracts for that template, the update part of the process will be significantly faster and much less likely to contend.


Ill-Matched Use Cases
=====================

By contrast, many Daml applications can yield patterns in the ACS and transactions that hurt the performance of applications built on the JSON API.
Below are some "gotchas" that might indicate that your application calls for a custom view, perhaps even stored locally in SQL and managed by your application, beyond what JSON API's query store can provide.

1. Workflows that use the "state field" antipattern.
   This adds a filter on the relatively inefficient payload query that ought to instead be placed on the template ID.
   In addition, updates to the state field will needlessly contend with updates to contracts with the state you're interested in.
2. Queries that return a large percentage of active contracts against a given contract type ID and party set.
   If the query store cannot yield any benefit from letting JSON API ignore most contracts on each query, then it will spend more time updating its contract table than it would have spent simply reading from the gRPC API and filtering directly, so you might as well turn off the query store.
3. Templates with high churn, i.e. the active contracts during the last query are very unlikely to still be active.
   In such cases JSON API may spend so much time updating its contract table that it washes out any performance advantage from being able to SQL query it afterwards.
4. Contracts with highly-overlapping signatories and observers.
   When signatories and observers do not intersect, their updates never contend; the more this happens, the more likely updates for queries with different party-sets will contend.
