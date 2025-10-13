Enterprise
##########

.. wip::
   * CREATE A VIDEO / ANIMATED RECORDING

   * Review the downloading section.
     Add CN artifacts if there are any offerings on that,
     refer to https://docs.sync.global/ for OSS docs


Digital Asset’s enterprise products
***********************************

  Digital Asset offers an enterprise distribution of Canton as well as complementary application modules and services geared towards helping institutions bring robust financial assets, applications and services to Canton Network quickly. The offering includes:

**Connect**: An enterprise-grade validator node distribution with additional reliability and security features like high-availability deployments and Key Management Service (KMS) integrations.
**Build**: Additional integration components like PQS and Shell offer developers a faster path to high scale applications.
**DA Utilities**: Composable modules and ready-to-go services on the network take care of the core of tokenization-related builds for Canton network.
**Subnets**: Enterprise grade infrastructure components for running private subnets of Canton Network give institutions additional independence and controls needed to bring some use cases to Canton Network at scale.
**Support**: 24x7x365 SLA based support with escalation paths to the core developers of the Canton blockchain.
  
Enterprise or Open Source
*************************

All capabilities of the Canton blockchain are available using the Open Source distributions of Canton and the Daml SDK. The enterprise distribution offers additional accelerators, quality of life features, streamlined packaging, robustness and scalability enhancements, and enterprise-grade support which are essential for bridging mission-critical use cases to market fast. The differences and similarities are summarized below. Please contact sales@digitalasset.com for more information.

.. list-table:: Differences between Enterprise and Open Source Edition
  :widths: 50,25,25
  :header-rows: 1

  * - Capability
    - Enterprise
    - Open-Source
  * - All Canton blockchain capabilities
    - Yes
    - Yes
  * - Node performance
    - High
    - ?
  * - Daml SDK
    - Yes
    - Yes
  * - Access to all apps and services on Canton Network
    - Yes
    - Yes
  * - DA Utiliites: composable modules for accelerated application builds
    - Yes
    - No
  * - Node High Availability
    - :externalref:`Yes <ha_user_manual>`
    - No
  * - :externalref:`Participant Query Store <participant-query-store>`
    - Yes
    - No
  * - :externalref:`Daml Shell <daml-shell>`
    - Yes
    - No
  * - Node Resource Management
    - Yes
    - No
  * - Ledger Pruning
    - Yes
    - No
  * - :externalref:`AWS KMS, GCP KMS <configure-a-kms>` and :externalref:`KMS Driver <configure-a-driver-based-kms>`
    - Yes
    - No
  * - Packaged up private BFT synchronizer for private sub-networks
    - Yes
    - No (Global Synchronizer Distribution only)
  * - High-performance, highly available Database synchronizer for private sub-networks
    - Yes
    - No
  * - 24x7x365 SLA-based support
    - Yes
    - No


.. _downloading:

Obtain the distribution
**********************************

Enterprise releases are available for use provided you have an enteprise license, available on on request to sales@digitalasset.com, and can be downloaded from the relevant component location under `repository <http://europe-docker.pkg.dev/da-images-dev/public>`__.

