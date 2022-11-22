.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Architecture
############

Components
**********

A production setup of the *HTTP JSON API* involves the following components:

- the *HTTP JSON API* server
- the query store backend database server
- the ledger

The *HTTP JSON API* server exposes an API to interact with the Ledger. It uses JDBC to interact
with its underlying query store in order to cache and serve data efficiently.

The *HTTP JSON API* server releases are regularly tested with the tools described under :doc:`/ops/requirements`.

In production, we recommend running on a x86_64 architecture in a Linux
environment. This environment should have a Java SE Runtime Environment with minimum version as mentioned at :doc:`/ops/requirements`.
We recommend using PostgreSQL server as query-store, again with minimum version as mentioned at :doc:`/ops/requirements`.
