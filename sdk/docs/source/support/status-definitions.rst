.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Status Definitions
##################

Throughout the documentation, we use labels to mark features of APIs not yet deemed stable. This page gives meaning to those labels.

Early Access Features
*********************

Features or components covered by these docs are :ref:`Stable <status_definitions>` by default. :ref:`Stable <status_definitions>` features and components constitute Daml's "public API" in the sense of :ref:`Semantic Versioning <versioning>`. Feature and components that are not :ref:`Stable <status_definitions>` are called "Early Access" and called out explicitly.

Early Access features are opt-in whenever possible, needing to be activated with special commands or flags needing to be started up separately, or requiring the use of additional endpoints, for example.

Within the Early Access category, we distinguish three labels:

Labs

  Labs components and features are experiments, introduced for evaluation, testing, or project-internal use. There is no intent to develop them into a stable feature other than to see whether they add value and find uptake. They can be changed or discontinued without advance notice. They may be poorly documented and it is not recommended to start relying on them.

Alpha

  Alpha components and features are early preview versions of features being actively developed to become a stable part of the ecosystem. At the Alpha stage, they are not yet feature complete, may have poor runtime characteristics, are still subject to frequent change, and may not be fully documented. Alpha features can be evaluated, and used in PoCs, but should not yet be relied upon for large projects or production use where breakages or changes to APIs would be costly. 

Beta

  Beta components and features are preview versions of features that are close to maturity. They are characterized by being considered feature complete, and the APIs close to the final public APIs. It is relatively safe to build on Beta features as long as the documented caveats to runtime characteristics are understood and bugs and minor API adjustments are not too costly.

Deprecation
***********

In addition to being labelled Early Access, features and components can also be labelled "Deprecated". Deprecation follows a deprecation cycle laid out in the table below. The date of deprecation is documented in :doc:`overview`.

Deprecated features can be relied upon during the deprecation cycle to the same degree as their non-deprecated counterparts, but building on deprecated features may hinder an upgrade to new Daml versions following the deprecation cycle.

.. _status_definitions:

Comparison of Statuses
**********************

The table below gives a concise overview of the labels used for Daml features and components.

.. list-table:: Feature Maturities
   :widths: 10 20 20 20 20
   :header-rows: 1

   * -
     - Stable
     - Beta
     - Alpha
     - Labs
   * - **Functionality**
     - 
     - 
     -
     -
   * - Functional Completeness
     - Functionally complete
     - Considered functionally complete, but subject to change according to usability testing
     - MVP-level functionality covering at least a few core use-cases
     - Functionality covering one specific use-case it was made for
   * - **Non-functional Requirements**
     - 
     - 
     -
     -
   * - Performance
     - Unless stated otherwise, the feature can be used without concern about system performance.
     - Current performance impacts and expected performance for the stable release are documented.
     - Using the feature may have significant undocumented impact on overall system performance.
     - Using the feature may have significant undocumented impact on overall system performance.
   * - Compatibility
     - Compatibility is covered by :doc:`compatibility`.
     - Compatibility is covered by :doc:`compatibility`.
     - The feature may only work against specific Daml integrations, or specific API versions, including Early Access ones.
     - The feature may only work against specific Daml integrations, or specific API versions, including Early Access ones.
   * - Stability & Error Recovery
     - The feature is long-term stable and supports recovery fit for a production system.
     - No known reproducible crashes which can't be recovered from. There is still an expectation that new issues may be discovered.
     - The feature may not be stable and lack error recovery.
     - The feature may not be stable and lack error recovery.
   * - **Releases and Support**
     - 
     - 
     -
     -
   * - Distribution and Releases
     - Distributed as part of regular :doc:`releases <releases>`.
     - Distributed as part of regular :doc:`releases <releases>`.
     - Distributed as part of regular :doc:`releases <releases>`.
     - Releases and distribution may be separate.
   * - Support
     - Covered by standard commercial support terms. Hotfixes for critical bugs and security issues are available.
     - Not covered by standard commercial support terms. Receives bug- and security fixes with regular releases.
     - Not covered by standard commercial support terms. Receives bug- and security fixes with regular releases.
     - Not covered by standard commercial support terms. Only receives fixes with low priority.
   * - Deprecation
     - May be removed with any new major version 12 months after the date of deprecation.
     - May be removed with any new minor version 1 month after the date of deprecation.
     - May be removed without warning.
     - May be removed without warning.
   * - Covered by :ref:`Semantic Versioning <versioning>`
     - Yes, part of the "public API".
     - No, but breaking changes will be documented.
     - No, and changes may be poorly documented.
     - No, and changes may be poorly documented.
   * - **Documentation**
     - 
     - 
     -
     -
   * - Basic Use
     - Fully documented as part of main docs.
     - Fully documented as part of main docs.
     - Basic documentation as part of main docs.
     - Documentation may be sparse and separate from the main docs.
   * - API, Functionality, and Gaps
     - Fully documented as part of main docs.
     - Fully documented as part of main docs.
     - Rough indication of targeted functionality and current limitations.
     - May be undocumented.
   * - Compatibility
     - Covered by :doc:`compatibility`.
     - Covered by :doc:`compatibility`.
     - Current compatibility documented as part of main docs.
     - May be undocumented.
