.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Production Setup
################

The vast majority of the prior documentation focuses on ease of testing and running
the service in a dev environment. From a production perspective given the wide
variety of use-cases there is far less of an established framework for deploying
the *HTTP-JSON API* server. In this document we would try to list some recommendations for
production deployments.

The *HTTP-JSON API* server is a JVM application that by default uses an in-memory backend.
This in-memory backend setup is inefficient for larger datasets as for every query it
ends up fetching the entire active contract set for the templates referenced in that query.
For this reason for production setups at a minimum we recommend to use a database
as a *Query Store*, this will allow for more efficient caching of the data to improve
query performance. Details for enabling a query store are highlighted below.

Query Store
***********

.. note:: The Community Edition of Daml Connect only supports PostgreSQL backends for the *HTTP JSON API* server, but the Enterprise Edition also supports Oracle backends.

*Query Store* can be described as a cached search index and is useful for use-cases
where we need to query large active contract sets(ACS). The *HTTP-JSON API* server can be
configured with PostgreSQL/Oracle(Enterprise Edition only) as the query-store backend.

The *Query Store* is built by saving the state of the ACS up to the current ledger
offset. This allows the *HTTP-JSON API* to only request the delta on subsequent queries,
making it much faster than having to request the entire ACS every time.

Given the cache like semantics of the *Query Store* it is safe to drop and re-initialize the store at any point

For example to enable the PostgreSQL backend you can use the ``--query-store-jdbc-config`` flag, as shown below.

.. code-block:: shell

    daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 \
    --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,start-mode=start-only"

If you are not familiar with JDBC URLs, we recommend reading the `PostgreSQL JDBC documentation <https://jdbc.postgresql.org/documentation/head/connect.html>`__
for more information.

The ``start-mode`` above simplifies re-initializing the database as well as allowing
for usage of separate credentials.
For example you can run the *HTTP-JSON API* server with ``start-mode=create-only``
so that all the necessary tables are created using one set of credentials, you can
then start the server with ``start-mode=start-only`` to use these tables.

Alternatively using ``start-mode=create-if-needed-and-start`` will create the
tables if missing on startup, whereas using ``start-mode=create-and-start``
will re-initialize the database on startup.


.. note:: The full list of *Query Store* configuration flags supported can be seen by running ``daml json-api --help``.


Security and privacy
********************

For an *HTTP-JSON API* server, all data is maintained by the operator of the deployment.
Thus, it is their responsibility to ensure that the data abides by the necessary
regulations and confidentiality expectations.

It is recommended to use the tools documented by PostgreSQL to protect data at
rest and using a secure communication channel between the *HTTP-JSON API* server and the PostgreSQL server.

To protect data in transit and over untrusted networks, the *HTTP-JSON API* server provides
TLS support, to enable TLS you need to specify the private key for your server and the
certificate chain via ``daml json-api --pem server.pem --crt server.crt``. You can also
set a custom root CA certificate used to validate client certificates via ``--cacrt ca.crt``

For more details on secure DAML infrastructure setup please refer to this `reference implementation <https://github.com/digital-asset/ex-secure-daml-infra>`__


Architecture
************

Components
----------

A production setup of the *HTTP-JSON API* will involve the following components:

- the *HTTP-JSON API* server
- the *Query Store* backend database server
- the ledger

*HTTP-JSON API* server exposes an API to interact with the Ledger and it uses JDBC to interact
with its underlying *Query Store* for caching and serving data efficiently.

The *HTTP-JSON API* server releases are regularly tested with OpenJDK 8 on a x86_64 architecture,
with Ubuntu 20.04, macOS 11.5.2 and Windows Server 2016.

In production, we recommend running on a x86_64 architecture in a Linux
environment. This environment should have a Java SE Runtime Environment such
as OpenJDK JRE and must be compatible with OpenJDK version 1.8.0_202 or later.
We recommend using PostgreSQL server as query-store, most of our tests have
been done with servers running version > 10.


Scaling and Redundancy
**********************

.. note:: This section of the document only talks about scaling and redundancy setup for the *HTTP-JSON API* server. In all of the recommendations suggested below we assume that the JSON API always interacts with a single participant on the ledger.

We advise that the *HTTP-JSON API* server and *Query Store* components to have dedicated
computation and memory resources available to them. This can be achieved via
containerization or setting them up on independent physical servers. Ensure that the two
components are **physically co-located** to reduce network latency for
communication. The scaling and availability aspects heavily rely on the interactions between
the core components listed above.

With respect to scaling we recommend to follow the general advice in trying to
understand the bottlenecks and see if adding additional processing power/memory is beneficial.

The *HTTP-JSON API* can be scaled independently of its *Query Store*.
You can have any number of *HTTP-JSON API* instances talking to the same *Query Store*
(if, for example, your monitoring indicates that the *HTTP-JSON API* processing time is the bottleneck),
or have each HTTP JSON API instance talk to its own independent *Query Store*
(if the database response times are the bottleneck).

In the latter case, the Daml privacy model ensures that the *HTTP-JSON API* requests
are made using the user-provided token, thus the data stored in a given
*Query Store* will be specific to the set of parties that have made queries through
that specific *Query Store* instance (for a given template).
Therefore, if you do run with separate *Query Stores*, it may be useful to route queries
(using a reverse proxy server) based on requesting party (and possibly queried template),
which would minimize the amount of data in each *Query Store* as well as the overall
redundancy of said data.

Users may consider running PostgreSQL backend in a `high availability configuration <https://www.postgresql.org/docs/current/high-availability.html>`__.
The benefits of this are use-case dependent as this may be more expensive for
smaller active contract datasets, where re-initializing the cache is cheap and fast.

Finally we recommend using app orchestration utilities which monitor the health of the service
and perform subsequent operations to ensure availability. These utilities can use the
`healthcheck endpoints <https://docs.daml.com/json-api/index.html#healthcheck-endpoints>`__
provided by the *HTTP-JSON API* server. This can also be tied into supporting arbitrary
autoscaling implementation to ensure minimum number of *HTTP-JSON API* servers on
failures.


Logging
*******

*HTTP-JSON API* server uses the industry-standard Logback for logging. You can
read more about that in the `Logback documentation <http://logback.qos.ch/>`__.

The logging infrastructure leverages structured logging as implemented by the
`Logstash Logback Encoder <https://github.com/logstash/logstash-logback-encoder/blob/logstash-logback-encoder-6.3/README.md>`__.

Logged events should carry information about the request being served by the
*HTTP-JSON API* server. This includes the details of the commands being submitted, the endpoints
being hit and response received highlighting details of failures if any.
When using a traditional logging target (e.g. standard output
or rotating files) this information will be part of the log description.
Using a logging target compatible with the Logstash Logback Encoder allows to have rich
logs with structured information about the event being logged.

The default log encoder used is the plaintext one for traditional logging targets.



Metrics
*******

Enable and configure reporting
------------------------------


To enable metrics and configure reporting, you can use the two following CLI options:

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

- ``--metrics-reporting-interval``: metrics are pre-aggregated on the *HTTP-JSON API* and sent to
  the reporter, this option allows the user to set the interval. The formats accepted are based
  on the ISO 8601 duration format ``PnDTnHnMn.nS`` with days considered to be exactly 24 hours.
  The default interval is 10 seconds.

Types of metrics
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

List of metrics
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