.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

Daml Sandbox
############

The Daml Sandbox, or Sandbox for short, is a simple ledger implementation that enables rapid application prototyping by simulating a Daml Ledger.

You can start Sandbox together with :doc:`Navigator </tools/navigator/index>` using the ``daml start`` command in a Daml project. This command will compile the Daml file and its dependencies as specified in the ``daml.yaml``. It will then launch Sandbox passing the just obtained DAR packages. The script specified in the ``init-script`` field in ``daml.yaml`` will be loaded into the ledger. Finally, it launches the navigator connecting it to the running Sandbox.

It is possible to execute the Sandbox launching step in isolation by typing ``daml sandbox``.

Sandbox can also be run manually as in this example:

.. code-block:: none

  $ daml sandbox --dar Main.dar --static-time
  Starting Canton sandbox.
  Listening at port 6865
  Uploading .daml/dist/foobar-0.0.1.dar to localhost:6865
  DAR upload succeeded.
  Canton sandbox is ready.

Behind the scenes, Sandbox spins up a Canton ledger with an in-memory
participant ``sandbox`` and an in-memory domain ``local``. You can pass additional
Canton configuration files via ``-c``. This option can be specified multiple times and
the resulting configuration files will be merged.

.. code-block:: none

   $ daml sandbox -c path/to/canton/config

.. _sandbox-authorization:

Run With Authorization
**********************

By default, Sandbox accepts all valid ledger API requests without performing any request authorization.

To start Sandbox with authorization using `JWT-based <https://jwt.io/>`__
access tokens as described in the
:doc:`Authorization documentation </app-dev/authorization>`, create a
config file that specifies the type of
authorization service and the path to the certificate.

.. code-block:: none
   :caption: auth.conf

   canton.participants.sandbox.ledger-api.auth-services = [{
       // type can be
       //   jwt-rs-256-crt
       //   jwt-es-256-crt
       //   jwt-es-512-crt
       type = jwt-rs-256-crt
       certificate = my-certificate.cert
   }]

- ``jwt-rs-256-crt``.
  The sandbox will expect all tokens to be signed with RS256 (RSA Signature with SHA-256) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``jwt-es-256-crt``.
  The sandbox will expect all tokens to be signed with ES256 (ECDSA using P-256 and SHA-256) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``jwt-es-512-crt``.
  The sandbox will expect all tokens to be signed with ES512 (ECDSA using P-521 and SHA-512) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

Instead of specifying the path to a certificate, you can also a
`JWKS <https://tools.ietf.org/html/rfc7517>`__ URL. In that case, the
sandbox will expect all tokens to be signed with RS256 (RSA Signature
with SHA-256) with the public key loaded from the given JWKS URL.

.. code-block:: none
   :caption: auth.conf

   canton.participants.sandbox.ledger-api.auth-services = [{
       type = jwt-rs-256-jwks
       url = "https://path.to/jwks.key"
   }]

.. warning::

  For testing purposes only, you can also specify a shared secret. In
  that case, the sandbox will expect all tokens to be signed with
  HMAC256 with the given plaintext secret. This is not considered safe for production.

.. code-block:: none
   :caption: auth.conf

   canton.participants.sandbox.ledger-api.auth-services = [{
       type = unsafe-jwt-hmac-256
       secret = "not-safe-for-production"
   }]

.. note:: To prevent man-in-the-middle attacks, it is highly recommended to use
          TLS with server authentication as described in :ref:`sandbox-tls` for
          any request sent to the Ledger API in production.

Generate JSON Web Tokens (JWT)
==============================

To generate access tokens for testing purposes, use the `jwt.io <https://jwt.io/>`__ web site.


Generate RSA keys
=================

To generate RSA keys for testing purposes, use the following command

.. code-block:: none

  openssl req -nodes -new -x509 -keyout sandbox.key -out sandbox.crt

which generates the following files:

- ``sandbox.key``: the private key in PEM/DER/PKCS#1 format
- ``sandbox.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Generate EC keys
================

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

Run With TLS
************

To enable TLS, you need to specify the private key for your server and
the certificate chain. This enables TLS for both the Ledger API and
the Canton Admin API. When enabling client authentication, you also
need to specify client certificates which can be used by Canton’s
internal processes. Note that the identity of the application
will not be proven by using this method, i.e. the `application_id` field in the request
is not necessarily correlated with the CN (Common Name) in the certificate.
Below, you can see an example config. For more details on TLS, refer to
:ref:`Canton’s documentation on TLS <tls-configuration>`.


.. code-block:: none
   :caption: tls.conf

   canton.participants.sandbox.ledger-api {
     tls {
       // the certificate to be used by the server
       cert-chain-file = "./tls/participant.crt"
       // private key of the server
       private-key-file = "./tls/participant.pem"
       // trust collection, which means that all client certificates will be verified using the trusted
       // certificates in this store. if omitted, the JVM default trust store is used.
       trust-collection-file = "./tls/root-ca.crt"
       // define whether clients need to authenticate as well (default not)
       client-auth = {
         // none, optional and require are supported
         type = require
         // If clients are required to authenticate as well, we need to provide a client
         // certificate and the key, as Canton has internal processes that need to connect to these
         // APIs. If the server certificate is trusted by the trust-collection, then you can
         // just use the server certificates. Otherwise, you need to create separate ones.
         admin-client {
           cert-chain-file = "./tls/admin-client.crt"
           private-key-file = "./tls/admin-client.pem"
         }
       }
     }
   }

Command-line Reference
**********************

To start Sandbox, run: ``daml sandbox [options] [-c canton.config]``.

To see all the available options, run ``daml sandbox --help``. Note
that this will show you the options of the Sandbox wrapper around
Canton. To see options of the underlying Canton runner, use
``daml sandbox --canton-help``.

Metrics
*******

Enable and Configure Reporting
==============================

You can enable metrics reporting via Prometheus using the following configuration file.

.. code-block:: none
   :caption: metrics.conf

   canton.monitoring.metrics.reporters = [{
     type = prometheus
     address = "localhost" // default
     port = 9000 // default
   }]

For other options and more details refer to the :ref:`Canton documentation <canton-metrics>`.

Types of Metrics
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

List of Metrics
===============

The following is a non-exhaustive list of selected metrics
that can be particularly important to track. Note that not
all the following metrics are available unless you run the
sandbox with a PostgreSQL backend.

``daml.commands.delayed_submissions``
-------------------------------------

A meter. Number of delayed submissions (submission that have been
evaluated to transaction with a ledger time farther in
the future than the expected latency).

``daml.commands.failed_command_interpretations``
------------------------------------------------

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

``daml.index.db.connection.api.server.pool``
--------------------------------------------

This namespace holds a number of interesting metrics about the
connection pool used to communicate with the persistent store
that underlies the index.

These metrics include:

- ``daml.index.db.connection.api.server.pool.Wait`` (timer): time spent waiting to acquire a connection
- ``daml.index.db.connection.api.server.pool.Usage`` (histogram): time spent using each acquired connection
- ``daml.index.db.connection.api.server.pool.TotalConnections`` (gauge): number or total connections
- ``daml.index.db.connection.api.server.pool.IdleConnections`` (gauge): number of idle connections
- ``daml.index.db.connection.api.server.pool.ActiveConnections`` (gauge): number of active connections
- ``daml.index.db.connection.api.server.pool.PendingConnections`` (gauge): number of threads waiting for a connection

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

``daml.services``
-----------------

Every metrics under this namespace is a timer, one for each
endpoint exposed by the index, read or write service. Metrics
are in the format:

``daml.services.service_name.service_endpoint``

The following example demonstrates a metric for transactions
submitted over the write service:

``daml.services.write.submit_transaction``

Single call services return the time to serve the request,
streaming services measure the time to return the first response.

``jvm``
-------

Under the ``jvm`` namespace there is a collection of metrics that
tracks important measurements about the JVM that the sandbox is
running on, including CPU usage, memory consumption and the
current state of threads.
