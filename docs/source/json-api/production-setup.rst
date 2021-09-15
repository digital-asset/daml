.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Production Setup
################

The vast majority of the prior documentation focuses on ease of testing and running
the service in a dev environment. From a production perspective given the wide
variety of use-cases there is far less of an established framework for deploying
the *JSON API*. In this document we would try to list some recommendations for
production deployments.

The *JSON API service* is a JVM application that by default uses an in-memory backend.
This in-memory backend setup is inefficient for larger datasets as for every query it
ends up fetching the entire active contract set for the templates referenced in that query.
For this reason for production setups at a minimum we recommend to use a database
as a query store, this will allow for more efficient caching of the data to improve
query performance. Details for enabling a query store are highlighted below.

Query Store
***********

Query store can be described as a cached search index and is useful for use-cases
where we need to query large active contract sets. The *JSON API* can be configured
with PostgreSQL/Oracle(Enterprise Edition only) as the query-store backend.

For e.g to enable the PostgreSQL backend you can use the ``--query-store-jdbc-config`` flag, an example of which is below.

.. note:: When you use the Query Store you'll want your first run to specify ``start-mode=create-only`` so that all the necessary tables are created. After the first run make sure ``start-mode=start-only`` so that it doesn't attempt to create the tables again.

.. code-block:: shell

    daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 \
    --query-store-jdbc-config "driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/test?&ssl=true,user=postgres,password=password,start-mode=start-only"


If you are not familiar with JDBC URLs, we recommend reading the `PostgreSQL JDBC documentation <https://jdbc.postgresql.org/documentation/head/connect.html>`__
for more information.


Service Configuration
*********************

.. note:: The full list of configuration flags supported by the JSON API can be seen by running ``daml json-api --help``.

Following are the list of recommended configs for a production deployment ? //TODO


Security and privacy
***************************

In *JSON API service*, all data is maintained by the operator of the deployment.
Thus, it is their responsibility to ensure that the data abides by the necessary
regulations and confidentiality expectations.

It is recommended to use the tools documented by PostgreSQL to protect data at
rest and using a secure communication channel between the *JSON API* server and the PostgreSQL server.

To protect data in transit and over untrusted networks, the JSON API provides
TLS support, to enable TLS you need to specify the private key for your server and the
certificate chain via ``daml json-api --pem server.pem --crt server.crt``. You can also
set a custom root CA certificate used to validate client certificates via ``--cacrt ca.crt``


Architecture
***************************

Components
----------

A production setup of the *JSON API* service will involve the following components:

- the *JSON API* server
- the query store backend database server
- a ledger

*JSON API* exposes a RESTFul API to interact with the Ledger and it uses JDBC to interact
with its underlying query store for caching and serving data efficiently.

The *JSON API* is regularly tested with OpenJDK 8 on a 64-bit x86 architecture,
with Ubuntu 20.04, macOS 11.5.2 and Windows Server 2016.

In production, we recommend running on a 64-bit x86 architecture in a Linux
environment. This environment should have a Java SE Runtime Environment such
as OpenJDK JRE and must be compatible with OpenJDK version 1.8.0_202 or later.
We recommend using PostgreSQL server as query-store, most of our tests have
been done with servers running version > 10.


Scaling and Redundancy
***************************

.. note:: This section of the document only talks about scaling and redundancy setup for the JSON API. In all of the recommendations suggested below we assume that the JSON API always interacts with a single participant on the ledger.

We advise that the *JSON API server* and *query store* components to have dedicated
computation and memory resources available to them. This can be achieved via
containerization or setting them up on independent physical servers. Ensure that the two
components are **physically co-located** to reduce network latency for
communication between the *JSON API* and the *database server*
The scaling and availability aspects heavily rely on the interactions between
the core components listed above.

With respect to scaling we recommend to follow the general advise in trying to
understand the bottlenecks and see if adding additional processing power/memory
i.e vertical scaling is beneficial.

In general for horizontal scaling purposes , we recommend to treat the
*JSON API* and the *query store database server* as a single unit of scaling.
While a setup with multiple *JSON API* services running against a single backend
database server is feasible, it maybe a futile exercise if the database server
itself is the bottleneck.

We can achieve redundancy for the JSON-API by using a load-balancer like nginx with some
acceptable routing mechanism and then and having multiple *JSON API* services sit behind it
each with their own backend database servers and dedicated computation and memory resources.

Users may consider running PostgreSQL backend in a `high availability configuration <https://www.postgresql.org/docs/current/high-availability.html>`__.
The benefits of this are use-case dependent as this may be more expensive for
smaller active contract datasets, where re-initializing the cache is cheap and fast.

Finally we recommend using app orchestration utilities which monitor the health of the service
and perform subsequent operations to ensure availability. These utilities can use the
`healthcheck endpoints <https://docs.daml.com/json-api/index.html#healthcheck-endpoints>`__
provided by the JSON API. This can also be tied into supporting arbitrary autoscaling implementation
to ensure minimum number of *JSON API* services on failures or rolling upgrades.


Logging
***************************

*JSON API* uses the industry-standard Logback for logging. You can
read more about that in the `Logback documentation <http://logback.qos.ch/>`__.

The logging infrastructure leverages structured logging as implemented by the
`Logstash Logback Encoder <https://github.com/logstash/logstash-logback-encoder/blob/logstash-logback-encoder-6.3/README.md>`__.

Logged events should carry information about the request being served by the
*JSON API* service. When using a traditional logging target (e.g. standard output
or rotating files) this information will be part of the log description.
Using a logging target compatible with the Logstash Logback Encoder allows to have rich
logs with structured information about the event being logged.



Metrics
***************************

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

- ``--metrics-reporting-interval``: metrics are pre-aggregated on the sandbox and sent to
  the reporter, this option allows the user to set the interval. The formats accepted are based
  on the ISO-8601 duration format ``PnDTnHnMn.nS`` with days considered to be exactly 24 hours.
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

A timer records all metrics registered by a meter and by an histogram, where
the histogram records the time necessary to execute a given operation (unless
otherwise specified, the precision is nanoseconds and the unit of measurement
is milliseconds).

List of metrics
===============

The following is an exhaustive list of selected metrics
that can be particularly important to track.

``daml.http_json_api.command_submission_timing``
------------------------------------------------

A timer. Meters how long processing of a command submission request takes

``daml.http_json_api.query_all_timing``
---------------------------------------

A timer. Meters how long processing of a query GET request takes

``daml.http_json_api.query_matching_timing``
--------------------------------------------

A timer. Meters how long processing of a query POST request takes

``daml.http_json_api.fetch_timing``
-----------------------------------

A timer. Meters how long processing of a fetch request takes

``daml.http_json_api.get_party_timing``
---------------------------------------

A timer. Meters how long processing of a get party/parties request takes

``daml.http_json_api.allocate_party_timing``
--------------------------------------------

A timer. Meters how long processing of a party management request takes

``daml.http_json_api.download_package_timing``
----------------------------------------------

A timer. Meters how long processing of a package download request takes

``daml.http_json_api.upload_package_timing``
--------------------------------------------

A timer. Meters how long processing of a package upload request takes

``daml.http_json_api.incoming_json_parsing_and_validation_timing``
------------------------------------------------------------------

A timer. Meters how long parsing and decoding of an incoming json payload takes

``daml.http_json_api.response_creation_timing``
-------------------------------------------------------

A timer. Meters how long the construction of the response json payload takes

``daml.http_json_api.response_creation_timing``
-------------------------------------------------------

A timer. Meters how long the construction of the response json payload takes

``daml.http_json_api.db_find_by_contract_key_timing``
-----------------------------------------------------

A timer. Meters how long a find by contract key database operation takes

``daml.http_json_api.db_find_by_contract_id_timing``
----------------------------------------------------

A timer. Meters how long a find by contract id database operation takes

``daml.http_json_api.command_submission_ledger_timing``
-------------------------------------------------------

A timer. Meters how long processing of the command submission request takes on the ledger

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