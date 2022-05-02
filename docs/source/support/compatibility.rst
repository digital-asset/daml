.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Portability, Compatibility, and Support Durations
#################################################

The Daml Ecosystem offers a number of forward and backward compatibility guarantees aiming to give the Ecosystem as a whole the following properties. See :ref:`ecosystem-architecture` for the terms used here and how they fit together.

Application Portability

  A Daml application should not depend on the underlying Database or DLT used by a Daml network.

Network Upgradeability

  Ledger Operators should be able to upgrade Daml network or Participant Nodes seamlessly to stay up to date with the latest features and fixes. A Daml application should be able to operate without significant change across such Network Upgrades.

Daml Upgradeability

  Application Developers should be able to update their developer tools seamlessly to stay up to date with the latest features and fixes, and stay able to maintain and develop their existing applications.

Ledger API Compatibility: Application Portability
*************************************************

Application Portability and to some extent Network Upgradeability are achieved by intermediating through the Ledger API. As per :ref:`versioning`, and :ref:`ecosystem-architecture`, the Ledger API is independently semantically versioned, and the compatibility guarantees derived from that semantic versioning extend to the entire semantics of the API, including the behavior of Daml Packages on the Ledger. Since all interaction with a Daml Ledger happens through the Daml Ledger API, a Daml Application is guaranteed to work as long as the Participant Node exposes a compatible Ledger API version.

Specifically, if a Daml Application is built against Ledger API version X.Y.Z and a Participant Node exposes Ledger API version X.Y2.Z2, the application is guaranteed to work as long as Y2.Z2 >= Y.Z.

Participant Nodes advertise the Ledger API version they support via the :ref:`version service <version-service>`.

.. note:

  Before release 1.7, the Ledger API version exposed by the Participant Node matched the version of the integration kit and SDK they were released with. 

As a concrete example, Daml for Postgres 1.4.0 has the Participant Node integrated, and exposes Ledger API version 1.4.0 and the Daml for VMware Blockchain 1.0 Participant Nodes expose Ledger API version 1.6.0. So any application that runs on Daml for Postgres 1.4.0 will also run on Daml for VMware Blockchain 1.0.

List of Ledger API Versions Supported by Daml
=============================================

The below lists with which Daml version a new Ledger API version was introduced.

.. list-table::   
   :header-rows: 1

   * - Ledger API Version
     - Daml Version
   * - 2.0
     - 2.0
   * - 1.12
     - 1.15
   * - 1.11
     - 1.14
   * - 1.10
     - 1.11
   * - 1.9
     - 1.10
   * - 1.8
     - 1.9
   * - <= 1.7
     - Introduced with the same Daml SDK version

Summary of Ledger API Changes
=============================

.. list-table::
   :header-rows: 1

   * - Ledger API Version
     - Changes
   * - 2.0
     - | Introduce User Management Service
       | Introduce Metering Report Service
       | Remove Reset Service
       | Deprecate Ledger Identity Service
       | Make ledger_id and application_id fields optional
       | Change error codes returned by the gRPC services
   * - 1.12
     - Introduce Daml-LF 1.14
   * - 1.11
     - Introduce Daml-LF 1.13
   * - 1.10
     - Introduce Daml-LF 1.12

       Stabilize :ref:`participant pruning <ops-ref_index>`
   * - 1.9
     - Introduce Daml-LF 1.11
   * - 1.8
     - Introduce Multi-Party Submissions
   * - <= 1.7
     - See Daml (SDK) `release notes <https://daml.com/release-notes>`_ of same version number.

Driver and Participant Compatibility: Network Upgradeability
************************************************************

Given the Ledger API Compatibility above, network upgrades are seamless if they preserve data, and Participant Nodes keep exposing the same or a newer minor version of the same major Ledger API Version. The semantic versioning of Daml drivers and participant nodes gives this guarantee. Upgrades from one minor version to another are data preserving, and major Ledger API versions may only be removed with a new major version of integration components, Daml drivers and Participant Nodes.

As an example, from an application standpoint, the only effect of upgrading Daml for Postgres 1.4.0 to Daml for Postgres 1.6.0 is an uptick in the Ledger API version. There may be significant changes to components or database schemas, but these are not public APIs. 

Participant database migration
==============================

Participant Nodes automatically manage their database schema. The database schema is tied to the Daml version, and schema migrations are always data preserving. The below lists which Daml version can be upgraded from which Daml version.

.. list-table::
   :header-rows: 1

   * - Daml SDK version
     - Upgradeable from
   * - 2.1
     - 1.7 or later
   * - <= 2.0
     - 1.0 or later

As an example, to upgrade a Participant Node built with Daml 1.4.0 to a version built with Daml 2.1, the operator should first upgrade to Daml 1.7 (or any other version between 1.7 and and 2.0), then upgrade to Daml 2.1.

SDK, Runtime Component, and Library Compatibility: Daml Upgradeability
**********************************************************************

As long as a major Ledger API version is supported (see :ref:`ledger-api-support`), there will be supported version of Daml able to target all minor versions of that major version. This has the obvious caveat that new features may not be available with old Ledger API versions.

For example, an application built and compiled with Daml SDK 1.4.0 against Ledger API 1.4.0, it can still be compiled using SDK 1.6.0 and can be run against Ledger API 1.4.0 using 1.6.0 libraries and runtime components. 

.. _ledger-api-support:

Ledger API Support Duration
***************************

Major Ledger API versions behave like stable features in :doc:`status-definitions`. They are supported from the time they are first released as "stable" to the point where they are removed from Integration Components and Daml following a 12 month deprecation cycle. The earliest point a major Ledger API version can be deprecated is with the release of the next major version. The earliest it can be removed is 12 months later with a major version release of the Integration Components.

Other than for hotfix releases, new releases of the Integration Components will only support the latest minor/patch version of each major Ledger API version.

As a result we can make this overall statement:

**An application built using Daml SDK U.V.W against Ledger API X.Y.Z can be maintained using any Daml SDK version U2.V2.W2 >= U.V.W as long as Ledger API major version X is still supported at the time of release of U2.V2.W2, and run against any Daml Network with Participant Nodes exposing Ledger API X.Y2.Z2 >= X.Y.Z.**
