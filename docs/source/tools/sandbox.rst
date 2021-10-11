.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

Daml Sandbox
############

The Daml Sandbox, or Sandbox for short, is a simple ledger implementation that enables rapid application prototyping by simulating a Daml Ledger.

You can start Sandbox together with :doc:`Navigator </tools/navigator/index>` using the ``daml start`` command in a Daml project. This command will compile the Daml file and its dependencies as specified in the ``daml.yaml``. It will then launch Sandbox passing the just obtained DAR packages. The script specified in the ``init-script`` field in ``daml.yaml`` will be loaded into the ledger. Finally, it launches the navigator connecting it to the running Sandbox.

It is possible to execute the Sandbox launching step in isolation by typing ``daml sandbox``.

Note: Sandbox has switched to use Wall Clock Time mode by default. To use Static Time Mode you can provide the ``--static-time`` flag to the ``daml sandbox`` command or configure the time mode for ``daml start`` in ``sandbox-options:`` section of ``daml.yaml``. Please refer to :ref:`Daml configuration files <daml-yaml-configuration>` for more information.

Sandbox can also be run manually as in this example:

.. code-block:: none

  $ daml sandbox Main.dar --static-time

     ____             ____
    / __/__ ____  ___/ / /  ___ __ __
   _\ \/ _ `/ _ \/ _  / _ \/ _ \\ \ /
  /___/\_,_/_//_/\_,_/_.__/\___/_\_\

  INFO: Initialized sandbox version 1.12.0-snapshot.20210312.6498.0.707c86aa with ledger-id = fd562651-5ebb-4a45-add7-25809ca1f297, port = 6865, dar file = List(Main.dar), time mode = static time, ledger = in-memory, auth-service = AuthServiceWildcard$, contract ids seeding = strong

Contract Identifier Generation
******************************

Sandbox supports two contract identifier generator schemes:

- The so-called *deterministic* scheme that deterministically produces
  contract identifiers from the state of the underlying ledger.  Those
  identifiers are strings starting with ``#``.

- The so-called *random* scheme that produces contract identifiers
  indistinguishable from random.  In practice, the schemes use a
  cryptographically secure pseudorandom number generator initialized
  with a truly random seed. Those identifiers are hexadecimal strings
  prefixed by ``00``.

The sandbox can be configured to use one or the other scheme with one
of the following command line options:

- ``--contract-id-seeding=<seeding-mode>``.  The Sandbox will use the
  seeding mode `<seeding-mode>` to seed the generation of random
  contract identifiers. Possible seeding modes are:

  - ``no``: The Sandbox uses the ``deterministic`` scheme. This is
    only supported by Sandbox classic and it prevents Sandbox from
    accepting packages in Daml-LF 1.11 or newer.

  - ``strong``: The Sandbox uses the ``random`` scheme initialized
    with a high-entropy seed.  Depending on the underlying operating
    system, the startup of the Sandbox may block as entropy is being
    gathered to generate the seed.

  - ``testing-weak``: (**For testing purposes only**) The Sandbox uses
    the ``random`` scheme initialized with a low entropy seed.  This
    may be used in a testing environment to avoid exhausting the
    system entropy pool when a large number of Sandboxes are started
    in a short time interval.

  - ``testing-static``: (**For testing purposes only**) The sandbox
    uses the ``random`` scheme with a fixed seed. This may be used in
    testing for reproducible runs.


Running with persistence
************************

Note: Running Sandbox with persistence is deprecated as of SDK 1.8.0 (16th Dec 2020). You can use the
Daml Driver for PostgreSQL instead.

By default, Sandbox uses an in-memory store, which means it loses its state when stopped or restarted. If you want to keep the state, you can use a Postgres database for persistence. This allows you to shut down Sandbox and start it up later, continuing where it left off.

To set this up, you must:

- create an initially empty Postgres database that the Sandbox application can access
- have a database user for Sandbox that has authority to execute DDL operations

  This is because Sandbox manages its own database schema, applying migrations if necessary when upgrading versions.

To start Sandbox using persistence, pass an ``--sql-backend-jdbcurl <value>`` option, where ``<value>`` is a valid jdbc url containing the username, password and database name to connect to.

Here is an example for such a url: ``jdbc:postgresql://localhost/test?user=fred&password=secret``

Due to possible conflicts between the ``&`` character and various terminal shells, we recommend quoting the jdbc url like so:

.. code-block:: none

  $ daml sandbox Main.dar --sql-backend-jdbcurl "jdbc:postgresql://localhost/test?user=fred&password=secret"

If you're not familiar with JDBC URLs, see the JDBC docs for more information: https://jdbc.postgresql.org/documentation/head/connect.html

.. _sandbox-authorization:

Running with authentication
***************************

By default, Sandbox does not use any authentication and accepts all valid ledger API requests.

To start Sandbox with authentication based on `JWT <https://jwt.io/>`__ tokens,
use one of the following command line options:

- ``--auth-jwt-rs256-crt=<filename>``.
  The sandbox will expect all tokens to be signed with RS256 (RSA Signature with SHA-256) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``--auth-jwt-es256-crt=<filename>``.
  The sandbox will expect all tokens to be signed with ES256 (ECDSA using P-256 and SHA-256) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``--auth-jwt-es512-crt=<filename>``.
  The sandbox will expect all tokens to be signed with ES512 (ECDSA using P-521 and SHA-512)     with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``--auth-jwt-rs256-jwks=<url>``.
  The sandbox will expect all tokens to be signed with RS256 (RSA Signature with SHA-256) with the public key loaded from the given `JWKS <https://tools.ietf.org/html/rfc7517>`__ URL.

.. warning::

  For testing purposes only, the following options may also be used.
  None of them is considered safe for production:

  - ``--auth-jwt-hs256-unsafe=<secret>``.
    The sandbox will expect all tokens to be signed with HMAC256 with the given plaintext secret.

Token payload
=============

JWTs express claims which are documented in the :ref:`authorization <authorization-claims>` documentation.

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

- ``ledgerId``, ``participantId``, ``applicationId`` restricts the validity of the token to the given ledger, participant, or application
- ``exp`` is the standard JWT expiration date (in seconds since EPOCH)
- ``admin``, ``actAs`` and ``readAs`` bear the same meaning as in the :ref:`authorization <authorization-claims>` documentation

The ``public`` claim is implicitly held by anyone bearing a valid JWT (even without being an admin or being able to act or read on behalf of any party).

Generating JSON Web Tokens (JWT)
================================

To generate tokens for testing purposes, use the `jwt.io <https://jwt.io/>`__ web site.


Generating RSA keys
===================

To generate RSA keys for testing purposes, use the following command

.. code-block:: none

  openssl req -nodes -new -x509 -keyout sandbox.key -out sandbox.crt

which generates the following files:

- ``sandbox.key``: the private key in PEM/DER/PKCS#1 format
- ``sandbox.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Generating EC keys
==================

To generate keys to be used with ES256 for testing purposes, use the following command

.. code-block:: none

  openssl req -x509 -nodes -days 3650 -newkey ec:<(openssl ecparam -name prime256v1) -keyout ecdsa256.key -out ecdsa256.crt

which generates the following files:

- ``ecdsa256.key``: the private key in PEM/DER/PKCS#1 format
- ``ecdsa256.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Similarly, you can use the following command for ES512 keys:

.. code-block:: none

  openssl req -x509 -nodes -days 3650 -newkey ec:<(openssl ecparam -name secp521r1) -keyout ecdsa512.key -out ecdsa512.crt

.. _sandbox-tls:

Running with TLS
****************

To enable TLS, you need to specify the private key for your server and
the certificate chain via ``daml sandbox --pem server.pem --crt
server.crt``.  By default, Sandbox requires client authentication as
well. You can set a custom root CA certificate used to validate client
certificates via ``--cacrt ca.crt``. You can change the client
authentication mode via ``--client-auth none`` which will disable it
completely, ``--client-auth optional`` which makes it optional or
specify the default explicitly via ``--client-auth require``.

Command-line reference
**********************

To start Sandbox, run: ``sandbox [options] <archive>...``.

To see all the available options, run ``daml sandbox --help``.

Metrics
*******

Enable and configure reporting
==============================

To enable metrics and configure reporting, you can use the two following CLI options:

- ``--metrics-reporter``: passing a legal value will enable reporting; the accepted values
  are ``console``, ``csv:</path/to/metrics.csv>`` and ``graphite:<local_server_port>``.

  - ``console``: prints captured metrics on the standard output

  - ``csv://</path/to/metrics.csv>``: saves the captured metrics in CSV format at the specified location

  - ``graphite://<server_host>[:<server_port>]``: sends captured metrics to a Graphite server. If the port
    is omitted, the default value ``2003`` will be used.

- ``--metrics-reporting-interval``: metrics are pre-aggregated on the sandbox and sent to
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
- ``<metric.qualified.name>.translation`` (timer): if relevant, time necessary to turn serialized Daml-LF values into in-memory objects

List of metrics
===============

The following is a non-exhaustive list of selected metrics
that can be particularly important to track. Note that not
all the following metrics are available unless you run the
sandbox with a PostgreSQL backend.

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
fed to the Daml interpreter.

``daml.commands.input_buffer_capacity``
----------------------------------------------------

A counter. The capacity of the queue accepting submissions on
the CommandService.

``daml.commands.input_buffer_length``
--------------------------------------------------

A counter. The number of currently pending submissions on
the CommandService.

``daml.commands.input_buffer_delay``
-------------------------------------------------

A timer. Measures the queuing delay for pending submissions
on the CommandService.

``daml.commands.max_in_flight_capacity``
-----------------------------------------------------

A counter. The capacity of the queue tracking completions on
the CommandService.

``daml.commands.max_in_flight_length``
---------------------------------------------------

A counter. The number of currently pending completions on
the CommandService.


``daml.execution.get_lf_package``
---------------------------------

A timer. Time spent by the engine fetching the packages of compiled
Daml code necessary for interpretation.

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

A database metric. Time spent loading a package of compiled Daml code
so that it's given to the Daml interpreter when
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
the Daml interpreter to evaluate a command into a
transaction.

``daml.index.db.lookup_configuration``
--------------------------------------

A database metric. Time to fetch the configuration so that it's
served via the configuration management service.

``daml.index.db.lookup_contract_by_key``
----------------------------------------

A database metric. Time to lookup one contract key on the index to be used by
the Daml interpreter to evaluate a command into a
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

A database metric. Time spent storing a Daml package uploaded through
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
tracks important measurements about the JVM that the sandbox is
running on, including CPU usage, memory consumption and the
current state of threads.
