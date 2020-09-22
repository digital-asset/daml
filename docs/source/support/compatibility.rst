.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Portability, Compatibility, and Support Durations
=================================================

The DAML Ecosystem offers a number of forward and backward compatibility guarantees aiming to give the Ecosystem as a whole the following properties. See :ref:`ecosystem-architecture` for the terms used here and how they fit together.

Portability

  A DAML application should not depend on the underlying Database or DLT.

Stability

  A DAML application should be able to operate without significant change over long periods of time.

Upgradeability

  Application Developers should be able to update their developer tools, and Ledger Operators should be able to upgrade DAML Network or Participant Nodes seamlessly to stay up to date with the latest features and fixes.

Ledger API Compatibility
------------------------

Portability and Stability of DAML Applications are achieved by intermediating through the Ledger API. As per :ref:`versioning`, and :ref:`ecosystem-architecture`, the Ledger API is independently semantically versioned, and the compatibility guarantees derived from that semantic versioning extend to the entire semantics of the API, including the behavior of DAML Packages on the Ledger. Since all interaction with a DAML Ledger happens through the DAML Ledger API, a DAML Application is guaranteed to work as long as the Participant Node exposes a compatible Ledger API version.

Specifically, if a DAML Application is built against Ledger API version X.Y.Z and a Participant Node exposes Ledger API version X.Y2.Z2, the application is guaranteed to work as long as Y2.Z2 >= Y.Z.

Currently, the latest Ledger API version is the same as the latest SDK, as everything gets released together as per :doc:`releases`, and there has been no need for the versions to diverge yet. This will likely change at the latest when one part of the ecosystem moves to version 2.X. Every DAML Driver advertises which Ledger API version it exposes.

As a concrete example, DAML for Postgres 1.4.0 exposes Ledger API version 1.4.0 and DAML for VMware Blockchain 1.0 exposes Ledger API version 1.6.0. So any application that runs on DAML for Postgres 1.4.0 will also run on DAML for VMware Blockchain 1.0, thus demonstrating both portability and stability.

Ledger API Support
------------------

Major Ledger API versions behave like stable features in :doc:`status-definitions`. They are supported from the time they are first released as "stable" to the point where they are removed from DAML Drivers and SDK following a 12 month deprecation cycle. The earliest point a major Ledger API version can be deprecated is with the release of the next major version. The earliest it can be removed, is 12 months later with a major version release of DAML Drivers and Participant Nodes.

Other than for hotfix releases, new releases of the DAML Drivers and Participant Nodes will only support the latest minor/patch version of each major Ledger API version.

Network Upgradeability
----------------------

Upgrades from one minor version of a stable DAML Network or Participant nodes are data preserving and have Ledger API backward compatibility as major Ledger API versions may only be removed in major versions of DAML Drivers or Participant Nodes. As an example, from an application standpoint, the only effect of upgrading DAML for Postgres 1.4.0 to DAML for Postgres 1.6.0 is an uptick in the Ledger API version. There may be significant changes to components or database schemas, but these are not public APIs, and the migrations happen automatically. 

SDK, Runtime Component, and Library Compatibility
-------------------------------------------------

As long as a major Ledger API version is supported, there will be supported versions of SDK, Runtime Components, and Libraries able to target all minor versions of that major version. This has the obvious caveat that new features may not be available with old Ledger API versions.

For example, an application built and compiled with SDK, Libraries and Runtime Components 1.4.0 against Ledger API 1.4.0, it still be compiled using SDK 1.6.0 and be run against Ledger API 1.4.0 using 1.6.0 libraries and runtime components. 

As a result we can make this statement:

**An application built using SDK, Libraries and Runtime Components U.V.W against Ledger API X.Y.Z can be maintained using any SDK, Library amd Runtime Components version U2.V2.W2 >= U.V.W as long as Ledger API major version X is still supported at the time of release of U2.V2.W2, and run against any DAML Network with Participant Nodes exposing Ledger API X.Y2.Z2 >= X.Y.Z.**
