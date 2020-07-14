.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

DAML Sandbox
############

The DAML Sandbox, or Sandbox for short, is a simple ledger implementation that enables rapid application prototyping by simulating a DAML Ledger.

You can start Sandbox together with :doc:`Navigator </tools/navigator/index>` using the ``daml start`` command in a DAML SDK project. This command will compile the DAML file and its dependencies as specified in the ``daml.yaml``. It will then launch Sandbox passing the just obtained DAR packages. Sandbox will also be given the name of the startup scenario specified in the project's ``daml.yaml``. Finally, it launches the navigator connecting it to the running Sandbox.

It is possible to execute the Sandbox launching step in isolation by typing ``daml sandbox``.

Note: Sandbox has switched to use Wall Clock Time mode by default. To use Static Time Mode you can provide the ``--static-time`` flag to the ``daml sandbox`` command or configure the time mode for ``daml start`` in ``sandbox-options:`` section of ``daml.yaml``. Please refer to :ref:`DAML configuration files <daml-yaml-configuration>` for more information.

Sandbox can also be run manually as in this example:

.. code-block:: none

  $ daml sandbox Main.dar --static-time --scenario Main:example

     ____             ____
    / __/__ ____  ___/ / /  ___ __ __
   _\ \/ _ `/ _ \/ _  / _ \/ _ \\ \ /
  /___/\_,_/_//_/\_,_/_.__/\___/_\_\
  initialized sandbox with ledger-id = sandbox-16ae201c-b2fd-45e0-af04-c61abe13fed7, port = 6865,
  dar file = DAR files at List(/Users/damluser/temp/da-sdk/test/Main.dar), time mode = Static, daml-engine = {}
  Initialized Static time provider, starting from 1970-01-01T00:00:00Z
  listening on localhost:6865

Here, ``daml sandbox`` tells the SDK Assistant to run ``sandbox`` from the active SDK release and pass it any arguments that follow. The example passes the DAR file to load (``Main.dar``) and the optional ``--scenario`` flag tells Sandbox to run the ``Main:example`` scenario on startup. The scenario must be fully qualified; here ``Main`` is the module and ``example`` is the name of the scenario, separated by a ``:``. We also specify that the Sandbox should run in Static Time mode so that the scenario can control the time.

.. note::

  The scenario is used for testing and development only, and is not supported by production DAML Ledgers. It is therefore inadvisable to rely on scenarios for ledger initialization.

  ``submitMustFail`` is only supported by the test-ledger used by ``daml test`` and the IDE, not by the Sandbox.

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

  - ``no``: The Sandbox uses the ``deterministic`` scheme.

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

.. _sandbox-authentication:

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
  and DER-encoded certicates (binary files) are supported.

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

JWTs express claims which are documented in the :ref:`authentication <authentication-claims>` documentation.

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
- ``admin``, ``actAs`` and ``readAs`` bear the same meaning as in the :ref:`authentication <authentication-claims>` documentation

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
specify the default explicitly via ``-.client-auth require``.

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
  - ``csv:</path/to/metrics.csv>``: saves the captured metrics in CSV format at the specified location
  - ``graphite:<local_server_port>``: sends captured metrics to a local Graphite server. If the port
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
the histogram records the time necessary to execute a given operation (measured
in milliseconds, unless otherwise specified).

Cache Metrics
-------------

A "cache metric" is a collection of simpler metrics that keep track of
relevant numbers about caches kept by the system to avoid re-running
expensive operations.

The metrics are:

- ``<metric.qualified.name>.size`` (gauge): instant measurement of the number of cached items
- ``<metric.qualified.name>.weight`` (gauge): instant measurement of the number of the (possibly approximate) size in bytes of cached items
- ``<metric.qualified.name>.hitCount`` (counter): how many times the cache was successfully accessed to retrieve an item
- ``<metric.qualified.name>.missCount`` (counter): how many times the cache did not have the required item and had to load it
- ``<metric.qualified.name>.loadSuccessCount`` (counter): how many times the cache successfully loaded an item so that it could be later served
- ``<metric.qualified.name>.loadFailureCount`` (counter): how many times the cache failed while trying to load an item
- ``<metric.qualified.name>.totalLoadTime`` (timer): overall time spent accessing the resource cached by this entity
- ``<metric.qualified.name>.evictionCount`` (counter): how many items have been evicted overall
- ``<metric.qualified.name>.evictionWeight`` (counter): (possibly approximate) size in bytes of overall evicted items

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

List of metrics
===============

The following is a (non-exhaustive) list of namespaces under
which you can find several metrics with a brief description
for each, accompanied by a list of selected metrics that can
be particularly important to track. Note that not all the
following metrics are available unless you run the sandbox
with a PostgreSQL backend.

``daml.commands``
-----------------

Statistics about the submission process of commands, including
validation, deduplication and delay.

``daml.commands.deduplicated_commands`` (meter)
``daml.commands.delayed_submissions`` (meter)
``daml.commands.failed_command_interpretation`` (meter)
``daml.commands.submissions`` (timer)
``daml.commands.valid_submissions`` (meter)
``daml.commands.validation`` (timer)

``daml.execution``
------------------

``daml.execution.get_lf_package`` (timer)
``daml.execution.lookup_active_contract_count_per_execution`` (timer)
``daml.execution.lookup_active_contract_per_execution`` (timer)
``daml.execution.lookup_active_contract`` (timer)
``daml.execution.lookup_contract_key_count_per_execution`` (histogram)
``daml.execution.lookup_contract_key_per_execution`` (timer)
``daml.execution.lookup_contract_key`` (timer)
``daml.execution.retry`` (meter)
``daml.execution.total`` (timer)

``daml.lapi``
-------------

Every metrics under this namespace is a timer, one for each
service exposed by the Ledger API, in the format:

    daml.lapi.service_name.service_endpoint

As in the following example:

    daml.lapi.command_service.submit_and_wait

Single call services return the time to serve the request,
streaming services measure the time to return the first response.

``daml.index.db``
-----------------

``daml.index.db.deduplicate_command`` (timer)
``daml.index.db.get_acs_event_sequential_id_range`` (timer)
``daml.index.db.get_active_contracts`` (timer)
``daml.index.db.get_completions`` (timer)
``daml.index.db.get_event_sequential_id_range`` (timer)
``daml.index.db.get_flat_transactions`` (timer)
``daml.index.db.get_initial_ledger_end`` (timer)
``daml.index.db.get_ledger_end`` (timer)
``daml.index.db.get_ledger_id`` (timer)
``daml.index.db.get_transaction_trees`` (timer)
``daml.index.db.initialize_ledger_parameters`` (timer)
``daml.index.db.load_all_parties`` (timer)
``daml.index.db.load_archive`` (timer)
``daml.index.db.load_configuration_entries`` (timer)
``daml.index.db.load_package_entries`` (timer)
``daml.index.db.load_packages`` (timer)
``daml.index.db.load_parties`` (timer)
``daml.index.db.load_party_entries`` (timer)
``daml.index.db.lookup_active_contract_with_cached_argument`` (timer)
``daml.index.db.lookup_active_contract`` (timer)
``daml.index.db.lookup_configuration`` (timer)
``daml.index.db.lookup_contract_by_key`` (timer)
``daml.index.db.lookup_flat_transaction_by_id`` (timer)
``daml.index.db.lookup_maximum_ledger_time`` (timer)
``daml.index.db.lookup_transaction_tree_by_id`` (timer)
``daml.index.db.remove_expired_deduplication_data`` (timer)
``daml.index.db.stop_deduplicating_command`` (timer)
``daml.index.db.store_configuration_entry`` (timer)
``daml.index.db.store_initial_state_from_scenario`` (timer)
``daml.index.db.store_ledger_entry`` (timer)
``daml.index.db.store_package_entry`` (timer)
``daml.index.db.store_party_entry`` (timer)
``daml.index.db.store_rejection`` (timer)
``daml.index.db.truncate_all_tables`` (timer)

``daml.indexer``
----------------

``daml.indexer.current_record_time_lag`` (gauge)
``daml.indexer.last_received_offset`` (gauge)
``daml.indexer.last_received_record_time`` (gauge)
``daml.indexer.processed_state_updates`` (timer)
