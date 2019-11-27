.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

DAML Sandbox
############

The DAML Sandbox, or Sandbox for short, is a simple ledger implementation that enables rapid application prototyping by simulating a Digital Asset Distributed Ledger. 

You can start Sandbox together with :doc:`Navigator </tools/navigator/index>` using the ``daml start`` command in a DAML SDK project. This command will compile the DAML file and its dependencies as specified in the ``daml.yaml``. It will then launch Sandbox passing the just obtained DAR packages. Sandbox will also be given the name of the startup scenario specified in the project's ``daml.yaml``. Finally, it launches the navigator connecting it to the running Sandbox.

It is possible to execute the Sandbox launching step in isolation by typing ``daml sandbox``.

Sandbox can also be run manually as in this example:

.. code-block:: none

  $ daml sandbox Main.dar --scenario Main:example

     ____             ____
    / __/__ ____  ___/ / /  ___ __ __
   _\ \/ _ `/ _ \/ _  / _ \/ _ \\ \ /
  /___/\_,_/_//_/\_,_/_.__/\___/_\_\
  initialized sandbox with ledger-id = sandbox-16ae201c-b2fd-45e0-af04-c61abe13fed7, port = 6865,
  dar file = DAR files at List(/Users/donkeykong/temp/da-sdk/test/Main.dar), time mode = Static, daml-engine = {}
  Initialized Static time provider, starting from 1970-01-01T00:00:00Z
  listening on localhost:6865

Here, ``daml sandbox`` tells the SDK Assistant to run ``sandbox`` from the active SDK release and pass it any arguments that follow. The example passes the DAR file to load (``Main.dar``) and the optional ``--scenario`` flag tells Sandbox to run the ``Main:example`` scenario on startup. The scenario must be fully qualified; here ``Main`` is the module and ``example`` is the name of the scenario, separated by a ``:``.

.. note::
  
  The scenario is used for testing and development only, and is not supported by production DAML Ledgers. It is therefore unadvisable to rely on scenarios for ledger initialization.

  ``submitMustFail`` is only supported by the test-ledger used by ``daml test`` and the IDE, not by the Sandbox.

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

To start Sandbox with authentication based on `JWT <https://jwt.io/>`_ tokens,
use one of the following command line options:

- ``--auth-jwt-rs256-crt=<filename>``.
  The sandbox will expect all tokens to be signed with RSA256 with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certicates (binary files) are supported.

- ``--auth-jwt-rs256-jwks=<url>``.
  The sandbox will expect all tokens to be signed with RSA256 with the public key loaded from the given `JWKS <https://tools.ietf.org/html/rfc7517>`_ URL.

.. warning::

  For testing purposes only, the following options may also be used.
  None of them is considered safe for production:

  - ``--auth-jwt-hss256-unsafe=<secret>``.
    The sandbox will expect all tokens to be signed with HMAC256 with the given plaintext secret.

Token payload
=============

The JWT payload has the following schema:

.. code-block:: json

   {
      "ledgerId": "aaaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      "participantId": null,
      "applicationId": null,
      "exp": 1300819380,
      "admin": true,
      "actAs": ["Alice"],
      "readAs": ["Bob"]
   }

where

- ``ledgerId``, ``participantId``, ``applicationId`` restricts the validity of the token to the given ledger, participant, or application
- ``exp`` is the standard JWT expiration date (in seconds since EPOCH)
- ``admin`` determines whether the token bearer is authorized to use admin endpoints of the ledger API
- ``actAs`` lists all DAML parties the token bearer can act as (e.g., as submitter of a command) and read data for
- ``readAs`` lists all DAML parties the token bearer can read data for

Generating tokens
=================

To generate tokens for testing purposes, use the `jtw.io <https://jwt.io/>`_ web site.

To generate RSA keys for testing purposes, use the following command

.. code-block:: none

  openssl req -nodes -new -x509  -keyout sandbox.key -out sandbox.crt

which generates the following files:

- ``sandbox.key``: the private key in PEM/DER/PKCS#1 format
- ``sandbox.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Command-line reference
**********************

To start Sandbox, run: ``sandbox [options] <archive>...``

To see all the available options, run ``daml sandbox --help``
