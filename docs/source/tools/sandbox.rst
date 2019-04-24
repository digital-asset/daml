.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _sandbox-manual:

DAML Sandbox
############

The DAML Sandbox, or Sandbox for short, is an in-memory ledger that enables rapid application prototyping by simulating the Digital Asset Distributed Ledger. It is a stand-alone JVM application utilizing the same components as the runtime platform.

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

Command-line reference
**********************

  Usage: sandbox [options] <archive>...
  
    -p, --port <value>     Sandbox service port. Defaults to 6865.
    -a, --address <value>  Sandbox service host. Defaults to binding on all addresses.
    --dalf                 This argument is present for backwards compatibility. DALF and DAR archives are now identified by their extensions.
    -s, --static-time      Use static time, configured with TimeService through gRPC.
    -w, --wall-clock-time  Use wall clock time (UTC). When not provided, static time is used.
    --no-parity            Legacy flag with no effect.
    --scenario <value>     If set, the sandbox will execute the given scenario on startup and store all the contracts created by it. Two formats are supported: Module.Name:Entity.Name (preferred) and Module.Name.Entity.Name (deprecated, will print a warning when used).
    <archive>...           Daml archives to load. Either in .dar or .dalf format. Only DAML-LF v1 Archives are currently supported.
    --pem <value>          TLS: The pem file to be used as the private key.
    --crt <value>          TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.
    --cacrt <value>        TLS: The crt file to be used as the the trusted root CA.
    --allow-dev            Allow usage of DAML-LF dev version. Do not use in production!
    --ledgerid <value>     Sandbox ledger ID. If missing, a random unique ledger ID will be used. Only useful with persistent stores.
    --help                 Print the usage text
