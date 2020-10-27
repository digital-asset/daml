.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Portability, Compatibility, and Support Durations
=================================================

The DAML Ecosystem offers a number of forward and backward compatibility guarantees aiming to give the Ecosystem as a whole the following properties. See :ref:`ecosystem-architecture` for the terms used here and how they fit together.

Application Portability

  A DAML application should not depend on the underlying Database or DLT used by a DAML network.

Network Upgradeability

  Ledger Operators should be able to upgrade DAML network or Participant Nodes seamlessly to stay up to date with the latest features and fixes. A DAML application should be able to operate without significant change across such Network Upgrades.

DAML Connect Upgradeability

  Application Developers should be able to update their developer tools seamlessly to stay up to date with the latest features and fixes, and stay able to maintain and develop their existing applications.

Ledger API Compatibility: Application Portability
-------------------------------------------------

Application Portability and to some extent Network Upgradeability are achieved by intermediating through the Ledger API. As per :ref:`versioning`, and :ref:`ecosystem-architecture`, the Ledger API is independently semantically versioned, and the compatibility guarantees derived from that semantic versioning extend to the entire semantics of the API, including the behavior of DAML Packages on the Ledger. Since all interaction with a DAML Ledger happens through the DAML Ledger API, a DAML Application is guaranteed to work as long as the Participant Node exposes a compatible Ledger API version.

Specifically, if a DAML Application is built against Ledger API version X.Y.Z and a Participant Node exposes Ledger API version X.Y2.Z2, the application is guaranteed to work as long as Y2.Z2 >= Y.Z.

Before SDK 1.7, the Ledger API version exposed by the Participant Node matches the SDK version. Currently, these versions can diverge. Integration Components, DAML Drivers, and Participant Nodes advertise the Ledger API version they expose.

As a concrete example, DAML for Postgres 1.4.0 has the Participant Node integrated, and exposes Ledger API version 1.4.0 and the DAML for VMware Blockchain 1.0 Participant Nodes expose Ledger API version 1.6.0. So any application that runs on DAML for Postgres 1.4.0 will also run on DAML for VMware Blockchain 1.0.

Driver and Participant Compatibility: Network Upgradeability
------------------------------------------------------------

Given the Ledger API Compatibility above, network upgrades are seamless if they preserve data, and Participant Nodes keep exposing the same or a newer minor version of the same major Ledger API Version. The semantic versioning of DAML drivers and participant nodes gives this guarantee. Upgrades from one minor version to another are data preserving, and major Ledger API versions may only be removed with a new major version of integration components, DAML drivers and Participant Nodes.

As an example, from an application standpoint, the only effect of upgrading DAML for Postgres 1.4.0 to DAML for Postgres 1.6.0 is an uptick in the Ledger API version. There may be significant changes to components or database schemas, but these are not public APIs. 

SDK, Runtime Component, and Library Compatibility: DAML Connect Upgradeability
------------------------------------------------------------------------------

As long as a major Ledger API version is supported (see :ref:`ledger-api-support`), there will be supported version of DAML Connect able to target all minor versions of that major version. This has the obvious caveat that new features may not be available with old Ledger API versions.

For example, an application built and compiled with DAML Connect 1.4.0 against Ledger API 1.4.0, it can still be compiled using SDK 1.6.0 and can be run against Ledger API 1.4.0 using 1.6.0 libraries and runtime components. 

.. _ledger-api-support:

Ledger API Support Duration
---------------------------

Major Ledger API versions behave like stable features in :doc:`status-definitions`. They are supported from the time they are first released as "stable" to the point where they are removed from Integration Components and DAML Connect following a 12 month deprecation cycle. The earliest point a major Ledger API version can be deprecated is with the release of the next major version. The earliest it can be removed is 12 months later with a major version release of the Integration Components.

Other than for hotfix releases, new releases of the Integration Components will only support the latest minor/patch version of each major Ledger API version.

As a result we can make this overall statement:

**An application built using DAML Connect U.V.W against Ledger API X.Y.Z can be maintained using any DAML Connect version U2.V2.W2 >= U.V.W as long as Ledger API major version X is still supported at the time of release of U2.V2.W2, and run against any DAML Network with Participant Nodes exposing Ledger API X.Y2.Z2 >= X.Y.Z.**
