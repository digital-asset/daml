Unifi
=====

Overview
--------

- CREATE A VIDEO / ANIMATED RECORDING
- REVIEW the open source vs enterprise split. right now its very canton focused. zero about utilties


Unifi is the enterprise platform built for all enterprises navigating the Canton Network.
It consists of ready-made solutions and services down to the infrastructure. Unifi provides
a set of tools and services that enable enterprises to build, deploy, and manage multi-party
applications on the Canton Network. It includes tokenization utilities, node infrastructure,
and network infrastructure.

Unifi services and products are:

- **Tokenization Utilities**: These are tools and services that enable enterprises to create,
manage, and trade digital assets on the Canton Network. They provide a secure and efficient
way to tokenize real-world assets and facilitate their transfer between parties.

- **SDK**: The Software Development Kit (SDK) is a set of tools and libraries to allow developers
to build applications for the Canton Network. Whether you integrate with an existing application
or whether you build one from scratch, the SDK provides the necessary tools to get started.

- **Participant**: The Participant is a core component of the Canton Network. Participants
are the nodes that run the distributed applications and maintain the privacy preserving
state of the network. The Unifi Participant is an enterprise grade version of its open source
counterpart, providing the necessary additional features and support required by enterprises.

- **Synchronizer**: The synchronizer is a component of the Canton Network that enables new
networks to be created to extend the existing network infrastructure. The Unifi Synchronizer
provides a set of highly available and highly performant synchronizers component that can be used
for high volume applications.


Unifi or Open Source
---------------------

The Canton distributed ledger is included with Unifi, or available as open source. Which role the application takes
depends on the configuration. The main administration interface of the Canton application is the embedded console, which
is part of the application.

Canton releases come in two variants: Open-Source or Enterprise. Both support the full Canton protocol, but differ in
terms of enterprise and non-functional capabilities:

.. list-table:: Differences between Enterprise and Open Source Edition
    :widths: 50,25,25
    :header-rows: 1

    * - Capability
      - Enterprise
      - Open-Source
    * - Daml Synchronisation
      - Yes
      - Yes
    * - Sub-Transaction Privacy
      - Yes
      - Yes
    * - Transaction Processing
      - Parallel (fast)
      - Sequential (slow)
    * - High Availability
      - :ref:`Yes <ha_user_manual>`
      - No
    * - `Participant Query Store <https://docs.daml.com/query/pqs-user-guide.html>`__
      - Yes
      - No
    * - `Daml Shell <https://docs.daml.com/2.9.1/tools/daml-shell/index.html>`__
      - Yes
      - No
    * - High Throughput via Microservices
      - :ref:`Yes <ha_user_manual>`
      - No
    * - Resource Management
      - Yes
      - No
    * - Ledger Pruning
      - Yes
      - No
    * - `AWS KMS, GCP KMS <https://docs.daml.com/canton/usermanual/kms/kms.html>`__ and  `KMS Driver <https://docs.daml.com/canton/usermanual/kms/kms_driver_guide.html>`__
      - Yes
      - No
    * - Postgres Backend
      - Yes
      - Yes

Please follow below instructions in order to obtain your copy of Canton.

.. _downloading:

Downloading the Enterprise Edition
----------------------------------

Enterprise releases are available on request (sales@digitalasset.com) and can be downloaded from the
respective `repository <https://digitalasset.jfrog.io/artifactory/canton-enterprise/>`__, or you can use
our Canton Enterprise Docker images as described in our :ref:`Docker instructions <docker-instructions>`.


Downloading the Open Source Edition
-----------------------------------

The Open Source release is available from `Github <https://github.com/digital-asset/daml/releases/latest>`__.
You can also use our Canton Docker images by following our :ref:`Docker instructions <docker-instructions>`.

