.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _deploy-ref_index:

Deploying to DAML ledgers
*************************

To run a DAML application, you'll need to deploy it to a DAML ledger.

How to deploy
=============

You can deploy to:

- The Sandbox with persistence. For information on how to do this, see the section on persistence in :doc:`/tools/sandbox` docs
- Further deployment options which are in development. For information on these options and their stage of development, see the table below.

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

.. _deploy-ref_in_development:

DAML ledgers built or in development
====================================

The following table lists the ledgers that support DAML, or are implementing
support for running DAML.

.. note: the table renderer fails *silently* if you don't have the right
   number of columns!

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - Ledger
     - Status
     - Developer (Managed Offering)
     - More information
   * - `VMware Blockchain <https://blogs.vmware.com/blockchain>`__
     - In development
     - `VMware <https://www.vmware.com/>`__
     - `press release, April 2019
       <http://hub.digitalasset.com/press-release/digital-asset-daml-smart-contract-language-now-extended-to-vmware-blockchain>`__
   * - `Hyperledger Sawtooth <https://sawtooth.hyperledger.org/>`__
     - In development
     - `Blockchain Technology Partners <https://blockchaintp.com/>`__
       (`Sextant <https://blockchaintp.com/sextant/>`__)
     - `press release, April 2019
       <https://www.hyperledger.org/blog/2019/04/16/daml-smart-contracts-coming-to-hyperledger-sawtooth>`__
   * - `Hyperledger Fabric <https://www.hyperledger.org/projects/fabric>`__
     - In development
     - `Hacera <https://hacera.com>`__
       (`Hacera Unbounded Network <https://unbounded.network/>`__)
     - `press release, June 2019
       <https://hub.digitalasset.com/press-release/digital-asset-announces-daml-partner-integrations-with-hyperledger-fabric-r3-corda-and-amazon-aurora>`__
   * - `R3 Corda <https://www.corda.net>`__
     - In development
     - `Digital Asset <https://digitalasset.com/>`__
     - `press release, June 2019
       <https://hub.digitalasset.com/press-release/digital-asset-announces-daml-partner-integrations-with-hyperledger-fabric-r3-corda-and-amazon-aurora>`__
   * - `Amazon Aurora <https://aws.amazon.com/rds/aurora/>`__
     - In development
     - `Blockchain Technology Partners <https://blockchaintp.com/>`__
       (`Sextant <https://blockchaintp.com/sextant/>`__)
     - `press release, June 2019
       <https://hub.digitalasset.com/press-release/digital-asset-announces-daml-partner-integrations-with-hyperledger-fabric-r3-corda-and-amazon-aurora>`__
   * - :doc:`/tools/sandbox`
     - Stable
     - `Digital Asset <https://digitalasset.com/>`__
     - supports `PostgreSQL <https://www.postgresql.org/>`__,
       see section on persistence in :doc:`/tools/sandbox` docs
   * - `Canton <https://www.canton.io>`__
     - In development
     - `Digital Asset <https://digitalasset.com/>`__
     - `www.canton.io <https://www.canton.io>`__, has native support for :doc:`DAML's fine-grained privacy model
       </concepts/ledger-model/ledger-privacy>`.
