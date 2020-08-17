.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

*DAML on SQL*
#############

*DAML on SQL* is a PostgreSQL-based DAML ledger implementation.

Setup PostgreSQL and run
************************

Before starting, you need to perform the following steps:

- create an initially empty PostgresSQL database that *DAML on SQL* can access
- create a database user for *DAML on SQL* that has authority to execute DDL
  operations

This is because *DAML on SQL* manages its own database schema, applying
migrations if necessary when upgrading versions.

To specify the PostgreSQL instance you wish to connect, use the
``--sql-backend-jdbcurl <value>`` command line option, where ``<value>`` is a
valid JDBC URL containing the username, password and database name to connect
to (for example, ``jdbc:postgresql://localhost/test?user=fred&password=secret``).

You will also need to provide a ledger ID with the `--ledgerid` flag, which must
be the same upon restart. This value is expected in many API endpoints, to
ensure ledger clients are connecting to the correct ledger.

Due to possible conflicts between the ``&`` character and various shells, we
recommend quoting the JDBC URL in the terminal, as follows:

.. code-block:: none

  $ java -jar daml-on-sql-<version>.jar --ledgerid=test --sql-backend-jdbcurl='jdbc:postgresql://localhost/test?user=fred&password=secret'

If you are not familiar with JDBC URLs, we recommend reading the `PostgreSQL JDBC documentation <https://jdbc.postgresql.org/documentation/head/connect.html>`__
for more information.

Architecture and availability
*****************************

Processes and components
========================

The core processes necessary to run a *DAML on SQL* deployment are:

- the *DAML on SQL* server and
- the PostgreSQL server used to persist the ledger data.

*DAML on SQL* communicates with the external world via the gRPC Ledger API
and communicates with PostgreSQL via JDBC to persist transactions, keep
track of active contracts, store compiled DAML packages, and so on.

Server hardware and software requirements
=========================================

*DAML on SQL* is provided as a self-contained JAR file, containing the
application and all dependencies. The application is routinely tested with
OpenJDK 8 on an x86 architecture, with Ubuntu 16.04, macOS 10.15, and Windows
Server 2016.

In production, we recommend running on an x86 architecture in a Linux
environment. Core requirements in such a situation include:

- a Java SE Runtime Environment such as OpenJDK JRE
  - the minimum supported Java version is 8
- OpenSSL 1.1 or later, made available to the above JRE
- glibc, made available to the above JRE

As a Java-based application, *DAML on SQL* can work on other operating systems
and architectures supporting a Java Runtime Environment. However, such an
environment will not have been tested and may cause issues.

Core architecture considerations
================================

The backing PostgreSQL server performs a lot of work which is both CPU- and
IO-intensive: all (valid) Ledger API requests will eventually hit the database.
At the same time, the *DAML on SQL* server has to have available resources to
validate requests, evaluate commands and prepare responses. While the PostgreSQL
schema is designed to be as efficient as possible, practical experience has
shown that having **dedicated computation and memory resources for the two core
components** (the *DAML on SQL* server and the PostgreSQL server) allows the two
to run without interfering with each other. Depending on the kind of deployment
you wish to make, this can be achieved with containerization, virtualization or
simply using physically different machines. Still, the Ledger API communicates
abundantly with the PostgreSQL server and many Ledger API requests need to go
all the way to persist information on the database. To reduce the latency
necessary to serve outstanding requests, **the *DAML on SQL* server and
PostgreSQL server should be physically co-located**.

Core availability considerations
================================

In order to address availability concerns, it's important to understand what
each of the core components do and how they interact with each other, in
particular regarding state and consistency.

Having two *DAML on SQL* servers running on top of a single PostgreSQL server
can lead to undefined (and likely broken) behavior. For this reason, it's
important to maintain a strict 1:1 relationship between a running *DAML on SQL
* server and a running PostgreSQL server. Note that using PostgreSQL in a high-
availability configuration does not allow you to run additional *DAML on SQL*
servers.

Downtime for the *DAML on SQL* server can be minimized using a watchdog or
orchestration system taking care of evaluating its health of the core components
and ensuring its availability. The Ledger API implementation of *DAML on SQL*
exposes the standard gRPC health checkpoint that can be used to evaluate the
health status of the Ledger API component. More information on the endpoint can
be found at the `documentation for gRPC <https://github.com/grpc/grpc/blob/1.29.0/doc/health-checking.md>`__.

When overloaded, the ledger will attempt to refuse additional requests, instead
responding with a ``RESOURCE_EXHAUSTED`` error. This error represents
*backpressure*, signaling to the client that they should back off and try again
later. Well-behaving clients will therefore allow the ledger to catch up with
outstanding tasks and resume normal operations.

Scale the ledger and associated services
========================================

*DAML on SQL* provides multiple configuration parameters to help tune for
availability and performance.

- ``--max-inbound-message-size``.
  You can use this parameter to increase (or decrease) the maximum size of a
  GRPC message. Often, DARs or transactions can become larger than the default
  of 4194304 bytes (4 MB). Increasing this will allow for larger transactions,
  at the expense of processing time.

- ``--events-page-size``.
  When streaming transactions, the API server will query the database in pages
  defaulting to a size of 1000. Increasing the page size can increase
  performance on servers with enough available memory.

- ``--max-commands-in-flight``.
  Increasing the maximum number of commands in flight will allow the API server
  to support more concurrent synchronous writes *per party*, at the expense of
  greater CPU and memory usage. The default maximum is 256, after which clients
  will receive a ``RESOURCE_EXHAUSTED`` error.

  Clients can also increase the number of concurrent requests by using the
  asynchronous endpoints for command submission and completion.

- ``--max-parallel-submissions``.
  Increasing the maximum number of parallel submissions from the default will
  allow for a larger queue of commands, but will also increase the CPU and
  memory demands of the ledger. The default maximum is 512, after which clients
  will receive a ``RESOURCE_EXHAUSTED`` error.

- ``--max-lf-value-translation-cache-entries``.
  In production, it's typical for many requests to be similar, resulting in
  the transaction verification and translation layer repeating a lot of work.
  Specifying a value for the translation cache allows the results of some of
  this repetitive work to be cached. The value represents the number of cached
  entries.

  This parameter can be tuned by observing its metrics, described below.

Security and privacy
********************

Trust assumptions
=================

In *DAML on SQL*, all data is kept centrally by the operator of the deployment.
Thus, it's their responsibility to ensure that the data is treated with the
appropriate care so to respect confidentiality and the applicable regulations.

The ledger operator is advised to use the tools available to them to not divulge
private user data, including those documented by PostgreSQL, to protect data at
rest and using a secure communication channel between the *DAML on SQL* server and
the PostgreSQL server.

Ledger API over TLS
===================

To protect data in transit and allow using the Ledger API over untrusted networks,
*DAML on SQL* leverages gRPC's built-in TLS support to allow clients to verify the
server's identity and encrypt the communication channel over which the Ledger API
requests and responses are sent.

To enable TLS, you need to specify the private key for your server and the certificate
chain via ``java -jar daml-on-sql-<version>.jar --pem server.pem --crt server.crt``.
By default, *DAML on SQL* requires client authentication as well. You can set a custom root
CA certificate used to validate client certificates via ``--cacrt ca.crt``. You can
change the client authentication mode via ``--client-auth none`` which will disable it
completely, ``--client-auth optional`` which makes it optional or specify the default
explicitly via ``--client-auth require``.

Ledger API Authorization
========================

By default, *DAML on SQL* accepts all valid Ledger API requests.

*DAML on SQL* allows to enable authorization, representing claims as defined by the
`Ledger API authorization documentation <https://docs.daml.com/app-dev/authentication.html#authentication-claims>`__
using the `JWT <https://jwt.io/>`__ format.

The following command line options are available to enable authorization:

- ``--auth-jwt-rs256-crt=<filename>``.
  *DAML on SQL* will expect all tokens to be signed with RS256 (RSA Signature with SHA-256)
  with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``--auth-jwt-es256-crt=<filename>``.
  *DAML on SQL* will expect all tokens to be signed with ES256 (ECDSA using P-256 and SHA-256)
  with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certicates (binary files) are supported.

- ``--auth-jwt-es512-crt=<filename>``.
  *DAML on SQL* will expect all tokens to be signed with ES512 (ECDSA using P-521 and SHA-512)
  with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``--auth-jwt-rs256-jwks=<url>``.
  *DAML on SQL* will expect all tokens to be signed with RS256 (RSA Signature with SHA-256)
  with the public key loaded from the given `JWKS <https://tools.ietf.org/html/rfc7517>`__ URL.

.. warning::

  For testing purposes only, the following option may also be used.
  None of them is considered safe for production:

  - ``--auth-jwt-hs256-unsafe=<secret>``.
    *DAML on SQL* will expect all tokens to be signed with HMAC256 with the given plaintext secret.

Token payload
^^^^^^^^^^^^^

The following is an example of a valid JWT payload:

.. code-block:: json

   {
      "https://daml.com/ledger-api": {
        "ledgerId": "aaaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "participantId": null,
        "applicationId": null,
        "admin": true,
        "actAs": ["Alice"],
        "readAs": ["Bob"]
      },
      "exp": 1300819380
   }

where

- ``ledgerId``, ``participantId``, ``applicationId`` restrict the validity of the token to the given ledger, participant, or application
- ``exp`` is the standard JWT expiration date (in seconds since Epoch)
- ``admin``, ``actAs`` and ``readAs`` bear the same meaning as in the Ledger API authorization documentation

The ``public`` claim is implicitly held by anyone bearing a valid JWT (even without being an admin or being able to act or read on behalf of any party).

Generate JSON Web Tokens (JWT)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To generate tokens for testing purposes, use the `jwt.io <https://jwt.io/>`__ web site.

Generate RSA keys
^^^^^^^^^^^^^^^^^

To generate RSA keys for testing purposes, use the following command

.. code-block:: none

  openssl req -nodes -new -x509 -keyout ledger.key -out ledger.crt

which generates the following files:

- ``ledger.key``: the private key in PEM/DER/PKCS#1 format
- ``ledger.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Generate EC keys
^^^^^^^^^^^^^^^^

To generate keys to be used with ES256 for testing purposes, use the following command

.. code-block:: none

  openssl req -x509 -nodes -days 3650 -newkey ec:<(openssl ecparam -name prime256v1) -keyout ecdsa256.key -out ecdsa256.crt

which generates the following files:

- ``ecdsa256.key``: the private key in PEM/DER/PKCS#1 format
- ``ecdsa256.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Similarly, you can use the following command for ES512 keys:

.. code-block:: none

  openssl req -x509 -nodes -days 3650 -newkey ec:<(openssl ecparam -name secp521r1) -keyout ecdsa512.key -out ecdsa512.crt

Command-line reference
**********************

To start *DAML on SQL*, run: ``java -jar daml-on-sql-<version>.jar [options] ``.

To see all the available options, run ``java -jar daml-on-sql-<version>.jar --help``.

Monitoring
**********

Configure logging
=================

*DAML on SQL* uses the industry-standard Logback for logging. You can
read more on how to set it up on the *DAML on SQL* CLI reference and
the `Logback documentation <http://logback.qos.ch/>`__.

Structured logging
^^^^^^^^^^^^^^^^^^

The *DAML on SQL* logging infrastructure leverages structured logging
as implemented by the `Logstash Logback Encoder <https://github.com/logstash/logstash-logback-encoder/blob/logstash-logback-encoder-6.3/README.md>`__.

Each logged event carries information about the request being served by
the Ledger API server (e.g. the command identifier). When using a
traditional logging target (e.g. standard output or rotating files) this
information will be part of the log description. Using a logging target
compatible with the Logstash Logback Encoder allows to have rich logs
with structured information about the event being logged.

Enable and configure reporting
==============================

To enable metrics and configure reporting, you can use the two following CLI options:

- ``--metrics-reporter``: passing a legal value will enable reporting; the accepted values
  are as follows:

  - ``console``: prints captured metrics to standard output

  - ``csv://</path/to/metrics.csv>``: saves the captured metrics in CSV format at the specified location

  - ``graphite://<server_host>[:<server_port>][/<metric_prefix>]``: sends captured metrics to a Graphite server. If the port
    is omitted, the default value ``2003`` will be used. A ``metric_prefix`` can be specified, causing all metrics to be reported with the specified prefix.

- ``--metrics-reporting-interval``: metrics are pre-aggregated on *DAML on SQL* and sent to
  the reporter, this option allows the user to set the interval. The formats accepted are based
  on the ISO-8601 duration format ``PnDTnHnMn.nS`` with days considered to be exactly 24 hours.
  The default interval is 10 seconds.

Types of metrics
================

This is a list of type of metrics with all data points recorded for each.
Use this as a reference when reading the list of metrics.

Gauge
-----

An individual instantaneous measurement.

Counter
-------

Number of occurrences of some event.

Meter
-----

A meter tracks the number of times a given event occurred. The following data
points are kept and reported by any meter.

- ``<metric.qualified.name>.count``: number of registered data points overall
- ``<metric.qualified.name>.m1_rate``: number of registered data points per minute
- ``<metric.qualified.name>.m5_rate``: number of registered data points every 5 minutes
- ``<metric.qualified.name>.m15_rate``: number of registered data points every 15 minutes
- ``<metric.qualified.name>.mean_rate``: mean number of registered data points

Histogram
---------

An histogram records aggregated statistics about collections of events.
The exact meaning of the number depends on the metric (e.g. timers
are histograms about the time necessary to complete an operation).

- ``<metric.qualified.name>.mean``: arithmetic mean
- ``<metric.qualified.name>.stddev``: standard deviation
- ``<metric.qualified.name>.p50``: median
- ``<metric.qualified.name>.p75``: 75th percentile
- ``<metric.qualified.name>.p95``: 95th percentile
- ``<metric.qualified.name>.p98``: 98th percentile
- ``<metric.qualified.name>.p99``: 99th percentile
- ``<metric.qualified.name>.p999``: 99.9th percentile
- ``<metric.qualified.name>.min``: lowest registered value overall
- ``<metric.qualified.name>.max``: highest registered value overall

Histograms only keep a small *reservoir* of statistically relevant data points
to ensure that metrics collection can be reasonably accurate without being
too taxing resource-wise.

Unless mentioned otherwise all histograms (including timers, mentioned below)
use exponentially decaying reservoirs (i.e. the data is roughly relevant for
the last five minutes of recording) to ensure that recent and possibly
operationally relevant changes are visible through the metrics reporter.

Note that ``min`` and ``max`` values are not affected by the reservoir sampling policy.

You can read more about reservoir sampling and possible associated policies
in the `Dropwizard Metrics library documentation <https://metrics.dropwizard.io/4.1.2/manual/core.html#man-core-histograms/>`__.

Timers
------

A timer records all metrics registered by a meter and by an histogram, where
the histogram records the time necessary to execute a given operation (unless
otherwise specified, the precision is nanoseconds and the unit of measurement
is milliseconds).

Database Metrics
----------------

A "database metric" is a collection of simpler metrics that keep track of
relevant numbers when interacting with a persistent relational store.

These metrics are:

- ``<metric.qualified.name>.wait`` (timer): time to acquire a connection to the database
- ``<metric.qualified.name>.exec`` (timer): time to run the query and read the result
- ``<metric.qualified.name>.query`` (timer): time to run the query
- ``<metric.qualified.name>.commit`` (timer): time to perform the commit
- ``<metric.qualified.name>.translation`` (timer): if relevant, time necessary to turn serialized DAML-LF values into in-memory objects

Cache Metrics
-------------

A "cache metric" is a collection of simpler metrics that keep track of
relevant numbers when interacting with an in-memory cache.

These metrics are:

- ``<metric.qualified.name>.hits`` (counter): the number of cache hits
- ``<metric.qualified.name>.misses`` (counter): the number of cache misses
- ``<metric.qualified.name>.load_successes`` (counter): the number of times a new value is successfully loaded into the cache
- ``<metric.qualified.name>.load_failures`` (counter): the number of times a new value fails to be loaded into the cache
- ``<metric.qualified.name>.load_total_time`` (timer): the total time spent loading new values into the cache
- ``<metric.qualified.name>.evictions`` (counter): the number of cache evictions
- ``<metric.qualified.name>.evicted_weight`` (counter): the total size of the values evicted from the cache
- ``<metric.qualified.name>.size`` (gauge): the size of the cache
- ``<metric.qualified.name>.weight`` (gauge): the total size of all values currently in the cache

List of metrics
===============

The following is a non-exhaustive list of selected metrics
that can be particularly important to track.

``daml.commands.deduplicated_commands``
---------------------------------------

A meter. Number of deduplicated commands.

``daml.commands.delayed_submissions``
-------------------------------------

A meter. Number of delayed submissions (submission who have been
evaluated to transaction with a ledger time farther in
the future than the expected latency).

``daml.commands.failed_command_interpretation``
-----------------------------------------------

A meter. Number of commands that have been deemed unacceptable
by the interpreter and thus rejected (e.g. double spends)

``daml.commands.submissions``
-----------------------------

A timer. Time to fully process a submission (validation,
deduplication and interpretation) before it's handed over
to the ledger to be finalized (either committed or rejected).

``daml.commands.valid_submissions``
-----------------------------------

A meter. Number of submission that pass validation and are
further sent to deduplication and interpretation.

``daml.commands.validation``
----------------------------

A timer. Time to validate submitted commands before they are
fed to the DAML interpreter.


``daml.execution.get_lf_package``
---------------------------------

A timer. Time spent by the engine fetching the packages of compiled
DAML code necessary for interpretation.

``daml.execution.lookup_active_contract_count_per_execution``
-------------------------------------------------------------

A histogram. Number of active contracts fetched for each processed transaction.

``daml.execution.lookup_active_contract_per_execution``
-------------------------------------------------------

A timer. Time to fetch all active contracts necessary to process each transaction.

``daml.execution.lookup_active_contract``
-----------------------------------------

A timer. Time to fetch each individual active contract during interpretation.

``daml.execution.lookup_contract_key_count_per_execution``
----------------------------------------------------------

A histogram. Number of contract keys looked up for each processed transaction.

``daml.execution.lookup_contract_key_per_execution``
----------------------------------------------------

A timer. Time to lookup all contract keys necessary to process each transaction.

``daml.execution.lookup_contract_key``
--------------------------------------

A timer. Time to lookup each individual contract key during interpretation.

``daml.execution.retry``
------------------------

A meter. Overall number of interpretation retries attempted due to
mismatching ledger effective time.

``daml.execution.total``
------------------------

A timer. Time spent interpreting a valid command into a transaction
ready to be submitted to the ledger for finalization.

``daml.index.db.connection.sandbox.pool``
-----------------------------------------

This namespace holds a number of interesting metrics about the
connection pool used to communicate with the persistent store
that underlies the index.

These metrics include:

- ``daml.index.db.connection.sandbox.pool.Wait`` (timer): time spent waiting to acquire a connection
- ``daml.index.db.connection.sandbox.pool.Usage`` (histogram): time spent using each acquired connection
- ``daml.index.db.connection.sandbox.pool.TotalConnections`` (gauge): number or total connections
- ``daml.index.db.connection.sandbox.pool.IdleConnections`` (gauge): number of idle connections
- ``daml.index.db.connection.sandbox.pool.ActiveConnections`` (gauge): number of active connections
- ``daml.index.db.connection.sandbox.pool.PendingConnections`` (gauge): number of threads waiting for a connection

``daml.index.db.deduplicate_command``
-------------------------------------

A timer. Time spent persisting deduplication information to ensure the
continued working of the deduplication mechanism across restarts.

``daml.index.db.get_active_contracts``
--------------------------------------

A database metric. Time spent retrieving a page of active contracts to be
served from the active contract service. The page size is
configurable, please look at the CLI reference.

``daml.index.db.get_completions``
---------------------------------

A database metric. Time spent retrieving a page of command completions to be
served from the command completion service. The page size is
configurable, please look at the CLI reference.

``daml.index.db.get_flat_transactions``
---------------------------------------

A database metric. Time spent retrieving a page of flat transactions to be
streamed from the transaction service. The page size is
configurable, please look at the CLI reference.

``daml.index.db.get_ledger_end``
--------------------------------

A database metric. Time spent retrieving the current ledger end. The count for this metric is expected to
be very high and always increasing as the indexed is queried for the latest updates.

``daml.index.db.get_ledger_id``
-------------------------------

A database metric. Time spent retrieving the ledger identifier.

``daml.index.db.get_transaction_trees``
---------------------------------------

A database metric. Time spent retrieving a page of flat transactions to be
streamed from the transaction service. The page size is
configurable, please look at the CLI reference.

``daml.index.db.load_all_parties``
----------------------------------

A database metric. Load the currently allocated parties so that
they are served via the party management service.

``daml.index.db.load_archive``
------------------------------

A database metric. Time spent loading a package of compiled DAML code
so that it's given to the DAML interpreter when
needed.

``daml.index.db.load_configuration_entries``
--------------------------------------------

A database metric. Time to load the current entries in the log of
configuration entries. Used to verify whether a configuration
has been ultimately set.

``daml.index.db.load_package_entries``
--------------------------------------

A database metric. Time to load the current entries in the log of
package uploads. Used to verify whether a package
has been ultimately uploaded.

``daml.index.db.load_packages``
-------------------------------

A database metric. Load the currently uploaded packages so that
they are served via the package management service.

``daml.index.db.load_parties``
------------------------------

A database metric. Load the currently allocated parties so that
they are served via the party service.

``daml.index.db.load_party_entries``
------------------------------------

A database metric. Time to load the current entries in the log of
party allocations. Used to verify whether a party
has been ultimately allocated.

``daml.index.db.lookup_active_contract``
----------------------------------------

A database metric. Time to fetch one contract on the index to be used by
the DAML interpreter to evaluate a command into a
transaction.

``daml.index.db.lookup_configuration``
--------------------------------------

A database metric. Time to fetch the configuration so that it's
served via the configuration management service.

``daml.index.db.lookup_contract_by_key``
----------------------------------------

A database metric. Time to lookup one contract key on the index to be used by
the DAML interpreter to evaluate a command into a
transaction.

``daml.index.db.lookup_flat_transaction_by_id``
-----------------------------------------------

A database metric. Time to lookup a single flat transaction by identifier
to be served by the transaction service.

``daml.index.db.lookup_maximum_ledger_time``
--------------------------------------------

A database metric. Time spent looking up the ledger effective time of a
transaction as the maximum ledger time of all active
contracts involved to ensure causal monotonicity.

``daml.index.db.lookup_transaction_tree_by_id``
-----------------------------------------------

A database metric. Time to lookup a single transaction tree by identifier
to be served by the transaction service.

``daml.index.db.remove_expired_deduplication_data``
---------------------------------------------------

A database metric. Time spent removing deduplication information after the expiration
of the deduplication window. Deduplication information is persisted to
ensure the continued working of the deduplication mechanism across restarts.

``daml.index.db.stop_deduplicating_command``
--------------------------------------------

A database metric. Time spent removing deduplication information after the failure of a
command. Deduplication information is persisted to ensure the continued
working of the deduplication mechanism across restarts.

``daml.index.db.store_configuration_entry``
-------------------------------------------

A database metric. Time spent persisting a change in the ledger configuration
provided through the configuration management service.

``daml.index.db.store_ledger_entry``
------------------------------------

A database metric. Time spent persisting a transaction that has been
successfully interpreted and is final.

``daml.index.db.store_package_entry``
-------------------------------------

A database metric. Time spent storing a DAML package uploaded through
the package management service.

``daml.index.db.store_party_entry``
-----------------------------------

A database metric. Time spent storing party information as part of the
party allocation endpoint provided by the party
management service.

``daml.index.db.store_rejection``
---------------------------------

A database metric. Time spent persisting the information that a given
command has been rejected.

``daml.index.db.translation.cache``
-----------------------------------

A cache metric. Measurements around the optional DAML-LF value translation
cache.

``daml.lapi``
-------------

Every metrics under this namespace is a timer, one for each
service exposed by the Ledger API, in the format:

``daml.lapi.service_name.service_endpoint``

As in the following example:

``daml.lapi.command_service.submit_and_wait``

Single call services return the time to serve the request,
streaming services measure the time to return the first response.

``jvm``
-------

Under the ``jvm`` namespace there is a collection of metrics that
tracks important measurements about the JVM that *DAML on SQL* is
running on, including CPU usage, memory consumption and the
current state of threads.

DAML Ledger Model Compliance
****************************

*DAML on SQL* is tested regularly against the DAML Ledger API Test
Tool to verify that the ledger implements correctly the DAML semantics
and to check its performance envelope.

Semantics
=========

On top of bespoke unit and integration tests, the *DAML on SQL* is
thoroughly tested with the Ledger API Test Tool to ensure that the
implementation correctly implements the DAML semantics.

These tests check that all the services which are part of the Ledger
API behave as expected, with a particular attention to ensure that
issuing commands and reading transactions respect the confidentiality
and privacy guarantees defined by the DAML Ledger Model.

Performance envelope
====================

Furthermore, this implementation is regularly tested to comply
with the DAML Ledger Implementation Performance Envelope tests.

In particular, the tests are run to ensure that *DAML on SQL* can:

- process transactions as large as 1 MB
- have a tail latency no greater than 1 second when issuing 20 pings
- have a throughput of 20 pings per second

You can read more on performance tests in the documentation of the
`Ledger API Test Tool <https://docs.daml.com/tools/ledger-api-test-tool/index.html#performance-tests>`__.

Replicate performance envelope tests
====================================

The following setup has been used to run the performance envelope
tests:

- PostgreSQL server: a GCP Cloud SQL managed instance using PostgreSQL 12,
with a 1 vCPU, 3.75 GB of RAM, 250 MB/s of network throughput, a 10 GB SDD HD,
1.2 MB/s of R/W disk throughput, 8 RIOPS and 15 WIOPS, no automatic failover
or disk increase, default PostgreSQL 12 configuration.

- *DAML on SQL* server: a GCP N1-Standard-1 instance, with 1 vCPU, 3.75 GB
of RAM, Ubuntu 20.04 LTS (64 bit), 10 GB boot disk, OpenJDK 1.8.0_242

- Ledger API test tool client: a GCP F1-Micro instance, with 1 shared vCPU,
614 MB of RAM, Ubuntu 20.04 LTS (64 bit), 10 GB boot disk, OpenJDK 1.8.0_242

The three instances were in the same region and availability
zone to minimize the latency between the three.

The tests run to evaluate the performance envelope are:

- PerformanceEnvelope.Latency.1000ms
- PerformanceEnvelope.Throughput.TwentyOPS
- PerformanceEnvelope.TransactionSize.1000KB

Please refer to the documentation for the Ledger API Test Tool to learn
how to run these tests.
