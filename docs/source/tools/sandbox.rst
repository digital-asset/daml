.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

DAML Sandbox
############

The DAML Sandbox, or Sandbox for short, is a simple ledger implementation that enables rapid application prototyping by simulating the Digital Asset Distributed Ledger. 

You can start DAML Sandbox together with :doc:`Navigator </tools/navigator/index>` using the ``daml start`` command in a DAML SDK project. This command will compile the DAML file and its dependencies as specified in the ``da.yaml``. It will then launch Sandbox passing the just obtained DAR packages. Sandbox will also be given the name of the startup scenario specified in the project's ``da.yaml``. Finally, it launches the navigator connecting it to the running Sandbox.

It is possible to execute the Sandbox launching step in isolation by typing ``daml sandbox``.

Sandbox can also be run manually as in this example::

  $ daml sandbox Main.dar --scenario Main:example

     ____             ____
    / __/__ ____  ___/ / /  ___ __ __
   _\ \/ _ `/ _ \/ _  / _ \/ _ \\ \ /
  /___/\_,_/_//_/\_,_/_.__/\___/_\_\
  initialized sandbox with ledger-id = sandbox-16ae201c-b2fd-45e0-af04-c61abe13fed7, port = 6865, dar file = DAR files at List(/Users/donkeykong/temp/da-sdk/test/Main.dar), time mode = Static, daml-engine = {}
  Initialized Static time provider, starting from 1970-01-01T00:00:00Z
  listening on localhost:6865

Here, ``daml sandbox `` tells the SDK Assistant to run ``sandbox`` from the active SDK release and pass it any arguments that follow. The example passes the DAR file to load (``Main.dar``) and the optional ``--scenario`` flag tells Sandbox to run the ``Main:example`` scenario on startup. The scenario must be fully qualified; here ``Main`` is the module and ``example`` is the name of the scenario, separated by a ``:``. The scenario is used for testing and development; it is not run in production.


Running with persistence
************************

By default, Sandbox uses an in-memory store, which means it loses its state when stopped or restarted. If you want to keep the state, you can use a Postgres database for persistence. This allows you to shut down Sandbox and start it up later, continuing where it left off.

To set this up, you must:

- create an initially empty Postgres database that the Sandbox application can access 
- have a database user for Sandbox that has authority to execute DDL operations 

  This is because Sandbox manages its own database schema, applying migrations if necessary when upgrading versions. 

To start Sandbox using persistence, pass an ``--jdbcurl <value>`` option, where ``<value>`` is a valid jdbc url containing the username, password and database name to connect to.

Here is an example for such a url: ``jdbc:postgresql://localhost/test?user=fred&password=secret``

If you're not familiar with JDBC URLs, see the JDBC docs for more information: https://jdbc.postgresql.org/documentation/head/connect.html

Command-line reference
**********************

To start Sandbox, run: ``sandbox [options] <archive>...``

To see all the available options, run ``da run sandbox -- --help``
