.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Ecosystem Overview
=======================

.. toctree::
   :hidden:

   status-definitions
   component-statuses

This page is intended to give you an overview of the components that constitute the DAML Ecosystem, what status they are in, and how they fit together. It lays out DAML's "public API" in the sense of :ref:`Semantic Versioning <versioning>`, and is a prerequisite to understanding DAML's :doc:`compatibility`.

The pages :doc:`status-definitions` and :doc:`component-statuses` give a fine-grained view of what labels like "Alpha" and "Beta" mean, which components expose public APIs and what status they are in.

.. _ecosystem-architecture:

Architecture
------------

A high level view of the architecture of a DAML application or solution is helpful to make sense of how individual components, APIs and features fit into the DAML Stack.

.. figure:: architecture.png

The stack is segmented into two parts. DAML Drivers encompass those components which enable an infrastructure to run DAML Smart Contracts, turning it into a **DAML Network**. **DAML Connect** consists of everything developers and users need to connect to a DAML Network: the tools to build, deploy, integrate, and maintain a DAML Application. 

DAML Networks
~~~~~~~~~~~~~

DAML Drivers
............

At the bottom of every DAML Application is a DAML network, a distributed, or possibly centralized persistence infrastructure together with DAML drivers. DAML drivers enable the persistence infrastructure to act as a consensus, messaging, and in some cases persistence layer for DAML Applications. Most DAML drivers will have a public API, but there are no *uniform* public APIs on DAML drivers. This does not harm application portability since applications only interact with DAML networks through the Participant Node. A good example of a public API of a DAML driver is the command line interface of `DAML for Postgres <https://github.com/digital-asset/daml/blob/master/ledger/daml-on-sql/README.rst>`_. It's a public interface, but specific to the Postgres driver.

Integration Components
......................

DAML drivers and Participant Nodes share a lot of components between underlying DLTs or Databases. These shared components are called the Integration Components, or sometimes the :doc:`/daml-integration-kit/index`.

Participant Nodes
~~~~~~~~~~~~~~~~~

On top of, or integrated into the DAML Drivers sits a Participant Node, that has the primary purpose of exposing the DAML Ledger API. In the case of *integrated* DAML Drivers, the Participant Node usually interacts with the DAML Drivers through solution-specific APIs. In this case, Participant Nodes can only communicate with DAML Drivers of one DAML Network. In the case of *interoperable* DAML Drivers, the Participant Node communicates with the DAML Drivers through the uniform `Canton Protocol <https://www.canton.io/docs/stable/user-manual/index.html>`_. The Canton Protocol is versioned and has some cross-version compatibility guarantees, but is not a public API. So participant nodes may have public APIs like monitoring and logging, command line interfaces or similar, but the only *uniform* public API exposed by all Participant Nodes is the Ledger API.

Ledger API
~~~~~~~~~~

The Ledger API is the primary interface that offers forward and backward compatibility between DAML Networks and Applications (including DAML Connect components). As you can see in the diagram above, all interaction between components above the Participant Node and the Participant Node or DAML Network happen through the Ledger API. The Ledger API is a public API and offers the lowest level of access to DAML Ledgers supported for application use.

DAML Connect
~~~~~~~~~~~~

Runtime Components
..................

Runtime components are standalone components that run alongside Participant Nodes or Applications and expose additional services like query endpoints, automations, or integrations. Each Runtime Component has public APIs, which are covered in :doc:`component-statuses`. Typically there is a command line interface, and one or more "Runtime APIs" as indicated in the above diagram.

Libraries
.........

Libraries naturally provide public APIs in their target language, be it DAML, or secondary languages like JavaScript or Java. For details on available libraries and their interfaces, see :doc:`component-statuses`.

Generated Code
..............

The SDK allows the generation of code for some languages from a DAML Model. This generated code has public APIs, which are not independently versioned, but depend on the DAML Connect version and source of the generated code, like a DAML package. In this case, the version of the DAML Connect SDK used covers changes to the public API of the generated code.

Developer Tools / SDK
.....................

The DAML Connect SDK consists of the developer tools used to develop user code, both DAML and in secondary languages, to generate code, and to interact with running applications via Runtime, and Ledger API. The SDK has a broad public API covering the DAML Language, CLIs, IDE, and Developer tools, but few of those APIs are intended for runtime use in a production environment. Exceptions to that are called out on :doc:`component-statuses`.
