.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Production Setup
################

The vast majority of prior documentation focused on ease of testing and on setting up the service to run in a dev environment. From a production perspective, given the wide variety of use-cases, there is far less of an established framework for the deployment of an *HTTP JSON API* server. In this document we will make some recommendations for production deployments.

The *HTTP JSON API* server is a JVM application that by default uses an in-memory backend.
This in-memory backend setup is inefficient for larger datasets as every query ends up fetching the entire active contract set for all the templates the query references. For this reason, for production setups, we recommend, at a mimumum, that one use a database as a query store. This will allow for a more efficient caching of data and will improve query performance. Details for enabling a query store are highlighted below.

Query Store
***********

.. note:: Daml Open Source only supports PostgreSQL backends for the *HTTP JSON API* server, but Daml Enterprise also supports Oracle backends.

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
---------------

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

Security and Privacy
********************

For an *HTTP JSON API* server, all data is maintained by the operator of the deployment.
Thus, it is the operator's responsibility to ensure that the data contained abides by the necessary
regulations and confidentiality expectations.

We recommend using the tools documented by PostgreSQL to protect data at
rest, and using a secure communication channel between the *HTTP JSON API* server and the PostgreSQL server.

To protect data in transit and over untrusted networks, the *HTTP JSON API* server provides
TLS support. To enable TLS you need to specify both the private key for your server and the
certificate chain via the below config block that specifies the ``cert-chain-file``, ``private-key-file``. You can also set
a custom root CA certificate that will be used to validate client certificates via the ``trust-collection-file`` parameter.

.. code-block:: none

    ledger-api {
      address = "127.0.0.1"
      port = 6400
      tls {
        enabled = "true"
        // the certificate to be used by the server
        cert-chain-file = "cert-chain.crt"
        // private key of the server
        private-key-file = "pvt-key.pem"
        // trust collection, which means that all client certificates will be verified using the trusted
        // certificates in this store. if omitted, the JVM default trust store is used.
        trust-collection-file = "root-ca.crt"
      }
    }

Using the cli options (deprecated), you can specify tls options using``daml json-api --pem server.pem --crt server.crt``.
Custom root CA certificate can be set via ``--cacrt ca.crt``

For more details on secure Daml infrastructure setup please refer to this `reference implementation <https://github.com/digital-asset/ex-secure-daml-infra>`__


Architecture
************

Components
----------

A production setup of the *HTTP JSON API* will involve the following components:

- the *HTTP JSON API* server
- the query store backend database server
- the ledger

The *HTTP JSON API* server exposes an API to interact with the Ledger and it uses JDBC to interact
with its underlying query store in order to cache and serve data efficiently.

The *HTTP JSON API* server releases are regularly tested with OpenJDK 11 on a x86_64 architecture,
with Ubuntu 20.04, macOS 11.5.2, and Windows Server 2016.

In production, we recommend running on a x86_64 architecture in a Linux
environment. This environment should have a Java SE Runtime Environment such
as OpenJDK JRE and must be compatible with OpenJDK version 11.0.11 or later.
We recommend using PostgreSQL server as query-store. Most of our tests have
been done with servers running version > 10.


Scaling and Redundancy
**********************

.. note:: This section of the document only talks about scaling and redundancy setup for the *HTTP JSON API* server. In all recommendations suggested below we assume that the JSON API is always interacting with a single participant on the ledger.

We advise that the *HTTP JSON API* server and query store components have dedicated
computation and memory resources available to them. This can be achieved via
containerization or by setting them up on independent physical servers. Please ensure that the two
components are **physically co-located** to reduce network latency for
communication. The scaling and availability aspects heavily rely on the interactions between
the core components listed above.

With respect to scaling we recommend one follow general practice: Try to
understand the bottlenecks and see if adding additional processing power/memory helps.

The *HTTP JSON API* can be scaled independently of its query store.
You can have any number of *HTTP JSON API* instances talking to the same query store
(if, for example, your monitoring indicates that the *HTTP JSON API* processing time is the bottleneck),
or have each HTTP JSON API instance talk to its own independent query store
(if the database response times are the bottleneck).

In the latter case, the Daml privacy model ensures that the *HTTP JSON API* requests
are made using the user-provided token, thus the data stored in a given
query store will be specific to the set of parties that have made queries through
that specific query store instance (for a given template).
Therefore, if you do run with separate query stores, it may be useful to route queries
(using a reverse proxy server) based on requesting party (and possibly queried template),
which would minimize the amount of data in each query store as well as the overall
redundancy of said data.

Users may consider running PostgreSQL backend in a `high availability configuration <https://www.postgresql.org/docs/current/high-availability.html>`__.
The benefits of this are use-case dependent as this may be more expensive for
smaller active contract datasets, where re-initializing the cache is cheap and fast.

Finally we recommend using orchestration systems or load balancers which monitor the health of
the service and perform subsequent operations to ensure availability. These systems can use the
`healthcheck endpoints <https://docs.daml.com/json-api/index.html#healthcheck-endpoints>`__
provided by the *HTTP JSON API* server. This can also be tied into supporting an arbitrary
autoscaling implementation in order to ensure a minimum number of *HTTP JSON API* servers on
failures.

Set up the HTTP JSON API Service to work with Highly Available Participants
---------------------------------------------------------------------------

In case the participant node itself is configured to be highly available, depending on the setup you may want
to choose different approaches to connect to the participant nodes. In most setups, including those based on Canton,
you'll likely have an active participant node whose role can be taken over by a passive node in case the currently
active one drops. Just as for the *HTTP JSON API* itself, you can use orchestration systems or load balancers to
monitor the status of the participant nodes and have those point your (possibly highly-available) *HTTP JSON API*
nodes to the active participant node.

To learn how to run and monitor Canton with high availability, refer to the :ref:`Canton documentation <ha_arch>`.

Logging
*******

The *HTTP JSON API* server uses the industry-standard Logback for logging. You can
read more about it in the `Logback documentation <http://logback.qos.ch/>`__.

The logging infrastructure leverages structured logging as implemented by the
`Logstash Logback Encoder <https://github.com/logstash/logstash-logback-encoder/blob/logstash-logback-encoder-6.3/README.md>`__.

Logged events should carry information about the request being served by the
*HTTP JSON API* server. This includes the details of the commands being submitted, the endpoints
being hit, and the response received – highlighting details of failures if any.
When using a traditional logging target (e.g. standard output
or rotating files) this information will be part of the log description.
Using a logging target compatible with the Logstash Logback Encoder allows one to have rich
logs that come with structured information about the event being logged.

The default log encoder used is the plaintext one for traditional logging targets.

.. _json-api-metrics:

Metrics
*******

Enable and Configure Reporting
------------------------------


To enable metrics and configure reporting, you can use the below config block in application config

.. code-block:: none

    metrics {
      //Start a metrics reporter. Must be one of "console", "csv:///PATH", "graphite://HOST[:PORT][/METRIC_PREFIX]", or "prometheus://HOST[:PORT]".
      reporter = "console"
      //Set metric reporting interval , examples : 1s, 30s, 1m, 1h
      reporting-interval = 30s
    }

or the two following CLI options (deprecated):

- ``--metrics-reporter``: passing a legal value will enable reporting; the accepted values
  are as follows:

  - ``console``: prints captured metrics on the standard output

  - ``csv://</path/to/metrics.csv>``: saves the captured metrics in CSV format at the specified location

  - ``graphite://<server_host>[:<server_port>]``: sends captured metrics to a Graphite server. If the port
    is omitted, the default value ``2003`` will be used.

  - ``prometheus://<server_host>[:<server_port>]``: renders captured metrics
    on a http endpoint in accordance with the prometheus protocol. If the port
    is omitted, the default value ``55001`` will be used. The metrics will be
    available under the address ``http://<server_host>:<server_port>/metrics``.

- ``--metrics-reporting-interval``: metrics are pre-aggregated on the *HTTP JSON API* and sent to
  the reporter, this option allows the user to set the interval. The formats accepted are based
  on the ISO 8601 duration format ``PnDTnHnMn.nS`` with days considered to be exactly 24 hours.
  The default interval is 10 seconds.

Types of Metrics
================

This is a list of type of metrics with all data points recorded for each.
Use this as a reference when reading the list of metrics.

Counter
-------

Number of occurrences of some event.

Meter
-----

A meter tracks the number of times a given event occurred (throughput). The following data
points are kept and reported by any meter.

- ``<metric.qualified.name>.count``: number of registered data points overall
- ``<metric.qualified.name>.m1_rate``: number of registered data points per minute
- ``<metric.qualified.name>.m5_rate``: number of registered data points every 5 minutes
- ``<metric.qualified.name>.m15_rate``: number of registered data points every 15 minutes
- ``<metric.qualified.name>.mean_rate``: mean number of registered data points

Timers
------

A timer records all metrics registered by a meter and by a histogram, where
the histogram records the time necessary to execute a given operation (
in fractional milliseconds).

List of Metrics
===============

The following is a list of selected metrics that can be particularly
important to track.

``daml.http_json_api.command_submission_timing``
------------------------------------------------

A timer. Measures latency (in milliseconds) for processing of a command submission request.

``daml.http_json_api.query_all_timing``
---------------------------------------

A timer. Measures latency (in milliseconds) for processing of a query GET request.

``daml.http_json_api.query_matching_timing``
--------------------------------------------

A timer. Measures latency (in milliseconds) for processing of a query POST request.

``daml.http_json_api.fetch_timing``
-----------------------------------

A timer. Measures latency (in milliseconds) for processing of a fetch request.

``daml.http_json_api.get_party_timing``
---------------------------------------

A timer. Measures latency (in milliseconds) for processing of a get party/parties request.

``daml.http_json_api.allocate_party_timing``
--------------------------------------------

A timer. Measures latency (in milliseconds) for processing of a party management request.

``daml.http_json_api.download_package_timing``
----------------------------------------------

A timer. Measures latency (in milliseconds) for processing of a package download request.

``daml.http_json_api.upload_package_timing``
--------------------------------------------

A timer. Measures latency (in milliseconds) for processing of a package upload request.

``daml.http_json_api.incoming_json_parsing_and_validation_timing``
------------------------------------------------------------------

A timer. Measures latency (in milliseconds) for parsing and decoding of an incoming json payload

``daml.http_json_api.response_creation_timing``
-------------------------------------------------------

A timer. Measures latency (in milliseconds) for construction of the response json payload.

``daml.http_json_api.db_find_by_contract_key_timing``
-----------------------------------------------------

A timer. Measures latency (in milliseconds) of the find by contract key database operation.

``daml.http_json_api.db_find_by_contract_id_timing``
----------------------------------------------------

A timer. Measures latency (in milliseconds) of the find by contract id database operation.

``daml.http_json_api.command_submission_ledger_timing``
-------------------------------------------------------

A timer. Measures latency (in milliseconds) for processing the command submission requests on the ledger.

``daml.http_json_api.http_request_throughput``
----------------------------------------------

A meter. Number of http requests

``daml.http_json_api.websocket_request_count``
----------------------------------------------

A Counter. Count of active websocket connections

``daml.http_json_api.command_submission_throughput``
----------------------------------------------------

A meter. Number of command submissions

``daml.http_json_api.upload_packages_throughput``
-------------------------------------------------

A meter. Number of package uploads

``daml.http_json_api.allocation_party_throughput``
--------------------------------------------------

A meter. Number of party allocations
