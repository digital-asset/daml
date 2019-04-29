.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

DAML Sandbox
############

The DAML Sandbox, or Sandbox for short, is a simple ledger implementation that enables rapid application prototyping by simulating the Digital Asset Distributed Ledger. 

You can start DAML Sandbox together with :doc:`Navigator </tools/navigator/index>` using the ``da start`` command in a DAML SDK project. This command will compile the DAML file and its dependencies as specified in the ``da.yaml``. It will then launch Sandbox passing the just obtained DAR packages. Sandbox will also be given the name of the startup scenario specified in the project's ``da.yaml``. Finally, it launches the navigator connecting it to the running Sandbox.

It is possible to execute the Sandbox launching step in isolation by typing ``da sandbox``.

Sandbox can also be run manually as in this example::

  $ da run sandbox -- Main.dar --scenario Main:example

     ____             ____
    / __/__ ____  ___/ / /  ___ __ __
   _\ \/ _ `/ _ \/ _  / _ \/ _ \\ \ /
  /___/\_,_/_//_/\_,_/_.__/\___/_\_\
  initialized sandbox with ledger-id = sandbox-16ae201c-b2fd-45e0-af04-c61abe13fed7, port = 6865, dar file = DAR files at List(/Users/donkeykong/temp/da-sdk/test/Main.dar), time mode = Static, daml-engine = {}
  Initialized Static time provider, starting from 1970-01-01T00:00:00Z
  listening on localhost:6865

Here, ``da run sandbox --`` tells the SDK Assistant to run ``sandbox`` from the active SDK release and pass it any arguments that follow. The example passes the DAR file to load (``Main.dar``) and the optional ``--scenario`` flag tells Sandbox to run the ``Main:example`` scenario on startup. The scenario must be fully qualified; here ``Main`` is the module and ``example`` is the name of the scenario, separated by a ``:``. The scenario is used for testing and development; it is not run in production.


Running with persistence
************************

By default the Sandbox uses an in-memory store and as such it loses its state when stopped or restarted. There is a possibility to use a Postgres database for persistence, which allows the Sandbox to be shut down and continued later where it left off.

The application has to have access to an initially empty database, and the database user needs to have authority to execute DDL operations as the Sandbox manages its own database schema, applying migrations if necessary when upgrading versions. 

In order to start the Sandox using persistence you need to pass an ``--jdbcurl <value>`` option, where `<value>` is a valid jdbc url containing the username, password and database name to connect to. Here is an example for such a url: ``jdbc:postgresql://localhost/test?user=fred&password=secret``

More on jdbc urls can be found here: https://jdbc.postgresql.org/documentation/head/connect.html

Command-line reference
**********************

You can start the Sandbox by running: ``sandbox [options] <archive>...`` All the available options can be seen by running ``da run sandbox -- --help``
