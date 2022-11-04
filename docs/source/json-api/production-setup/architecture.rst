.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Architecture
############

Components
**********

A production setup of the *HTTP JSON API* will involve the following components:

- the *HTTP JSON API* server
- the query store backend database server
- the ledger

The *HTTP JSON API* server exposes an API to interact with the Ledger and it uses JDBC to interact
with its underlying query store in order to cache and serve data efficiently.

The *HTTP JSON API* server releases are regularly tested with OpenJDK 11 on a x86_64 architecture,
with Ubuntu 20.04, macOS 11.5.2, and Windows Server 2016.

In production, we recommend running on a x86_64 architecture in a Linux
environment. This environment should have a Java SE Runtime Environment such
as OpenJDK JRE and must be compatible with OpenJDK version 11.0.11 or later.
We recommend using PostgreSQL server as query-store. Most of our tests have
been done with servers running version > 10.
