.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Query Store
###########

.. note:: Daml Open Source only supports PostgreSQL backends for the *HTTP JSON API* server, but Daml Enterprise also supports Oracle backends.

The *HTTP JSON API* server is a JVM application that by default uses an in-memory backend.
This in-memory backend setup is inefficient for larger datasets as every query ends up fetching the entire active contract set for all the templates the query references. For this reason, for production setups, we recommend, at a minimum, that one use a database as a query store. This will allow for a more efficient caching of data and will improve query performance. Details for enabling a query store are highlighted below.

The query store is a cached search index and is useful for use cases
where the application needs to query large active contract sets (ACS). The *HTTP JSON API* server can be
configured with PostgreSQL/Oracle (Daml Enterprise only) as the query store backend.

The query store is built by saving the state of the ACS up to the current ledger
offset. This allows the *HTTP JSON API* to only request the delta on subsequent queries,
making it much faster than requesting the entire ACS every time.

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

You can also use the ``--query-store-jdbc-config`` CLI flag (deprecated), as shown below.

.. code-block:: shell

    daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 \
    --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,start-mode=start-only"

Consult your database vendor's JDBC driver documentation to learn how to specify a JDBC connection string that suits your needs.

The ``start-mode`` is a custom parameter, defined by the query store configuration itself, which allows one to deal
with the initialization and usage of the database backing the query store.

Depending on how you prefer to operate it, you can either choose to:

* run the *HTTP JSON API* server with ``start-mode=create-only`` with a user
  that has exclusive table-creating rights that are required for the query store
  to operate, and then start it once more with ``start-mode=start-only`` with a user
  that can use the aforementioned tables, but that cannot apply schema changes
* run the *HTTP JSON API* server with a user that can both create and use
  the query store tables by passing ``start-mode=create-and-start``

When restarting the *HTTP JSON API* server after a schema has already been
created, it's safe practice to always use ``start-mode=start-only``.

.. note:: You can see the full list of query store configuration flags supported by running ``daml json-api --help``.

Data Continuity
***************

The query store is a cache. This means that it is perfectly fine to drop it, as
the data it contains is a subset of what can safely be recovered from the ledger.

As such, the query store does not provide data continuity guarantees across versions
and furthermore doesn't guarantee that a query store initialized with a previous
version of the *HTTP JSON API* will be able to work with a newer version.

However, the *HTTP JSON API* is able to tolerate working with query stores initialized
by a previous version of the software so long as the underlying schema did not change.

The query store keeps track of the schema version under which it was initialized and
refuses to start if a new schema is detected when it's run with a newer version.

To evolve, the operator of the *HTTP JSON API* query store needs to drop the database
used to hold the *HTTP JSON API* query store, needs to create a new one (consult your database
vendor's documentation as to how this should be done), and then, depending on the operator's preferred production setup, should proceed to create and
start the server using either ``start-mode=create-only`` & ``start-mode=start-only``
or only with ``start-mode=create-and-start`` as described above.

