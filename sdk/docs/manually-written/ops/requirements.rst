.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ops-ref_index:

System Requirements
===================

Unless otherwise stated, all Daml runtime components require the following:

1. For development, an x86-compatible system running a modern Linux, Windows,
   or MacOS operating system
2. For production deployment, an x86-compatible system running a modern Linux
   operating system
3. Java 11 or later
4. An RDBMS system, *either*:

  * PostgreSQL 11.17-15 (12+ recommended)

    *or*

  * Oracle Database 19.11 or later

5. JDBC drivers compatible with your RDBMS

In terms of hardware requirements for development, a simple Daml application
can run with a laptop using 2 GB of memory and a couple of CPU cores.
However, a much larger environment (see Install Canton / Hardware Resources)  is recommended for testing or production
use.

Feature/Component System Requirements
-------------------------------------

`The JavaScript client libraries <../app-dev/bindings-ts/index.html>`_ are tested on Node 14.18.3 with TypeScript compiler 4.5.4. For best results, use these or later versions.
