.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Ledgers built or in development
************************************

To run a DAML application, you'll need to deploy it to a DAML ledger. The following table lists the ledgers that support DAML, or are implementing
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