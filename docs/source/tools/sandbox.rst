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

Sandbox requires the names of the input ``.dar`` or ``.dalf`` files as arguments to start.
The available command line options are listed here::

    -p, --port <value>       Sandbox service port. Defaults to 6865.
    -a, --address <value>    Sandbox service host. Defaults to binding on all addresses.
    --dalf                   This argument is present for backwards compatibility. DALF and DAR archives are now identified by their extensions.
    --static-time            Use static time, configured with TimeService through gRPC.
    -w, --wall-clock-time    Use wall clock time (UTC). When not provided, static time is used.
    --no-parity              Disables Ledger Server parity mode. Features which are not supported by the Platform become available.
    --scenario <value>       If set, the sandbox will execute the given scenario on startup and store all the contracts created by it. Two formats are supported: Module.Name:Entity.Name (preferred) and Module.Name.Entity.Name (deprecated, will print a warning when used).
    --daml-lf-archive-recursion-limit <value>
                             Set the recursion limit when decoding DAML-LF archives (.dalf files). Default is 1000
    <archive>...             Daml archives to load. Either in .dar or .dalf format. Only DAML-LF v1 Archives are currently supported.
    --help                   Print the usage text

Performance benchmarks
**********************

Sandbox's performance depends on many different factors. These include the hardware it is run on, the size of the specific models used in the test, and the characteristics of the applications used on the client side of the Ledger API. This means you should treat the numbers below merely as an indication of Sandbox's capabilities.

In the tests performed to date, Sandbox has been shown to: 

 * start in less than 3s
 * create 100,000 contracts within 12s window
 * consume less than 200,000MB of memory for the benchmark 100,000 contracts
 * support simultaneous connections from 10 clients all sending commands at around 1'000 contracts per second.
