.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _deploy-ref_index:

Deploying to DAML Ledgers
*************************

To run a DAML application, you'll need to deploy it to a DAML ledger.

How to Deploy
=============

You can deploy to:

- The Sandbox with persistence. For information on how to do this, see the section on persistence in :doc:`/tools/sandbox` docs.
- Other available DAML ledgers. For information on these options and their stage of development, see the :ref:`tables below <deploy-ref_available>`.

To deploy a DAML project to a ledger, you will need the ledger's hostname (or IP) and the port number for the gRPC Ledger API. The default port number is 6865. Then, inside your DAML project folder, run the following command, taking care to substitute the ledger's hostname and port for ``<HOSTNAME>`` and ``<PORT>``:

.. code-block:: none

   $ daml deploy --host=<HOSTNAME> --port=<PORT>

This command will deploy your project to the ledger. This has two steps:

#. It will allocate the parties specified in the project's ``daml.yaml`` on the ledger if they are missing. The command looks through the list of parties known to the ledger, sees if any party is missing by comparing display names, and adds any missing party via the party management service of the Ledger API.

#. It will upload the project's compiled DAR file to the ledger via the package management service of the Ledger API. This will make the templates defined in the current project available to the users of the ledger.

Instead of passing ``--host`` and ``--port`` flags to the command above, you can add the following section to the project's ``daml.yaml`` file:

.. code-block:: yaml

   ledger:
       host: <HOSTNAME>
       port: <PORT>

You can also use the ``daml ledger`` command for more fine-grained deployment options, and to interact with the ledger more generally. Try running ``daml ledger --help`` to get a list of available ledger commands:

.. code-block:: none

   $ daml ledger --help
   Usage: daml ledger COMMAND
     Interact with a remote DAML ledger. You can specify the ledger in daml.yaml
     with the ledger.host and ledger.port options, or you can pass the --host and
     --port flags to each command below.

   Available options:
     -h,--help                Show this help text

   Available commands:
     list-parties             List parties known to ledger
     allocate-parties         Allocate parties on ledger
     upload-dar               Upload DAR file to ledger
     navigator                Launch Navigator on ledger

.. _deploy-ref_available:

Available DAML Products
=======================

The following table lists commercially supported DAML ledgers and environments that are available for production use today.

.. list-table::
   :header-rows: 1

   * - Product
     - Ledger
     - Vendor
   * - `Sextant for DAML <https://blockchaintp.com/sextant/daml/>`__
     - `Amazon Aurora <https://aws.amazon.com/rds/aurora/>`__
     - `Blockchain Technology Partners <https://blockchaintp.com/>`__
   * - `Sextant for DAML <https://blockchaintp.com/sextant/daml/>`__
     - `Hyperledger Sawtooth <https://sawtooth.hyperledger.org/>`__
     - `Blockchain Technology Partners <https://blockchaintp.com/>`__
   * - `project : DABL <https://projectdabl.com/>`__
     - `Managed cloud enviroment <https://projectdabl.com/>`__
     - `Digital Asset <https://digitalasset.com/>`__

.. _deploy-ref_open_source:

Open Source Integrations
========================

The following table lists open source DAML integrations.

.. list-table::
   :header-rows: 1

   * - Ledger
     - Developer
     - More Information
   * - `Hyperledger Sawtooth <https://sawtooth.hyperledger.org/>`__
     - `Blockchain Technology Partners <https://blockchaintp.com/>`__
     - `Github Repo <https://github.com/blockchaintp/daml-on-sawtooth>`__
   * - `Hyperledger Fabric <https://www.hyperledger.org/projects/fabric>`__
     - `Hacera <https://hacera.com/>`__
     - `Github Repo <https://github.com/hacera/daml-on-fabric>`__
   * - `PostgreSQL <https://www.postgresql.org/>`__
     - `Digital Asset <https://digitalasset.com/>`__
     - `DAML Sandbox Docs <https://docs.daml.com/tools/sandbox.html>`__

.. _deploy-ref_in_development:

DAML Ledgers in Development
===========================

The following table lists the ledgers that are implementing support for running DAML.

.. list-table::
   :header-rows: 1

   * - Ledger
     - Developer
     - More Information
   * - `VMware Blockchain <https://blogs.vmware.com/blockchain>`__
     - `VMware <https://www.vmware.com/>`__
     - `Press release, April 2019 <http://hub.digitalasset.com/press-release/digital-asset-daml-smart-contract-language-now-extended-to-vmware-blockchain>`__
   * - `Corda <https://www.corda.net/>`__
     - `R3 <https://www.corda.net/>`__
     - `press release, June 2019 <https://hub.digitalasset.com/press-release/digital-asset-announces-daml-partner-integrations-with-hyperledger-fabric-r3-corda-and-amazon-aurora>`__
   * - `QLDB <https://aws.amazon.com/qldb/>`__
     - `Blockchain Technology Partners <https://blockchaintp.com/>`__
     - `press release, September 2019 <https://blog.daml.com/daml-driven/quantum-daml-amazon-qldb-goes-ga>`__
   * - `Canton <https://www.canton.io/>`__
     - `Digital Asset <https://digitalasset.com/>`__ reference implementation
     - `canton.io <https://www.canton.io/>`__

