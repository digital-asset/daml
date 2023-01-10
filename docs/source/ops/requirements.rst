.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ops-ref_index:

System Requirements
===================

Unless otherwise stated, all Daml runtime components require the following dependencies:

1. An x86-compatible system running a modern Unix, Windows, or MacOS operating system.
2. Java 11 or greater.
3. An RDBMS system,

  1. Either PostgreSQL 11.17 or greater.
  2. Or Oracle Database 19.11 or greater.

4. JDBC drivers compatible with the chosen RDBMS.

Daml is tested using the following specific dependencies in default installations.

1. Operating Systems:

  1. Ubuntu 20.04
  2. Windows Server 2016
  3. MacOS 10.15 Catalina

2. `Eclipse Adoptium <https://adoptium.net>`_ version 11 for Java.
3. PostgreSQL 11.17
4. Oracle Database 19.11

In terms of hardware requirements, minimal deployments running simple Daml applications
are regularly tested with as little as 2 GB of memory and access to a single, shared vCPU.

Feature/Component System Requirements
-------------------------------------

1. :doc:`The JavaScript Client Libraries <../app-dev/bindings-ts/index>` are tested on Node 14.18.3. with typescript compiler 4.5.4. Versions greater or equal to these are recommended.
