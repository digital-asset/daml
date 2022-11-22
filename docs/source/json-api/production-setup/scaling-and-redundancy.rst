.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Scaling and Redundancy
######################

.. note:: This section of the document only talks about scaling and redundancy setup for the *HTTP JSON API* server. In all recommendations suggested below we assume that the JSON API is always interacting with a single participant on the ledger.

We recommend dedicating computation and memory resources to the *HTTP JSON API* server and query store components. This can be achieved via
containerization or by setting these components up on independent physical servers. Make sure that the two
components are **physically co-located** to reduce network latency for
communication. Scaling and availability heavily rely on the interactions between
the core components listed above.

The general principles of scaling apply here: Try to
understand the bottlenecks and see if adding additional processing power/memory helps.


Scaling creates and exercises
*****************************

The HTTP JSON API service provides simple, synchronous endpoints for carrying out creates and exercises on the ledger.
It does not support the complex multi-command asynchronous submission protocols supported by the ledger API.

For performing large numbers of creates and exercises at once, while you can perform many HTTP requests at once to carry out this task, it may be simpler and more concurrent-safe to shift more of this logic into a Daml choice that can be exercised.

The pattern looks like this:

1. Have a contract with a key and one or more choices on the ledger.
2. Such a choice can carry out as many creates and exercises as desired; all of these will take place in a single transaction.
3. Use the HTTP JSON API service to exercise this choice by key.

It's possible to go too far in the other direction: any error will usually cause the whole transaction to roll back, so an excessively large amount of work done by a single choice can also cause needless retrying.
You can solve this by batching requests, or using :doc:`/daml/intro/8_Exceptions` to collect and return failed cases to the HTTP JSON API service client for retrying, allowing successful parts of the batch to proceed.


Scaling Queries
***************

The :doc:`query-store` is a key factor of efficient queries.
However, it behaves very differently depending on the characteristics of the underlying ledger, Daml application, and client query patterns.
:doc:`Understanding how it works <query-store>` is a major prerequisite to understanding how the HTTP JSON API service will interact with your application's performance profile.

Additionally, the *HTTP JSON API* can be scaled independently of its query store.
You can have any number of *HTTP JSON API* instances talking to the same query store
(if, for example, your monitoring indicates that the *HTTP JSON API* processing time is the bottleneck),
or have each HTTP JSON API instance talk to its own independent query store
(if the database response times are the bottleneck).

In the latter case, the Daml privacy model ensures that the *HTTP JSON API* requests
are made using the user-provided token, thus the data stored in a given
query store will be specific to the set of parties that have made queries through
that specific query store instance (for a given template).
Therefore, if you do run with separate query stores, it may be useful to route queries
(using a reverse proxy server) based on requesting party (and possibly queried template),
which would minimize the amount of data in each query store as well as the overall
redundancy of said data.

Users may consider running PostgreSQL backend in a `high availability configuration <https://www.postgresql.org/docs/current/high-availability.html>`__.
The benefits of this are use-case dependent as this may be more expensive for
smaller active contract datasets, where re-initializing the cache is cheap and fast.

Finally, we recommend using orchestration systems or load balancers which monitor the health of
the service and perform subsequent operations to ensure availability. These systems can use the
`healthcheck endpoints <https://docs.daml.com/json-api/index.html#healthcheck-endpoints>`__
provided by the *HTTP JSON API* server. This can also be tied into supporting an arbitrary
autoscaling implementation in order to ensure a minimum number of *HTTP JSON API* servers on
failures.


Hitting a Scaling Bottleneck
****************************

As HTTP JSON API service and its query store are optimized for rapid application development and ease of developer onboarding, you may reach a point where your application's performance demands exceed what the HTTP JSON API service can offer.
The more demanding your application is, the less likely it is to be well-matched with the simplifications and generalizations that the HTTP JSON API service makes for developer simplicity.

In this case, it's important to remember that *the HTTP JSON API service can only do whatever an ordinary ledger API client application could do, including your own*.

For example, for a JVM application, interacting with JSON is probably simpler than gRPC directly, but using :doc:`/app-dev/bindings-java/index` :doc:`codegen </app-dev/bindings-java/codegen>` are much simpler than either.

There is no way to make :doc:`query-store` more suited to high-performance queries for your Daml application than a custom data store implemented as your own server on gRPC would be.
So an application that *must* interact over JSON, but requires very high-performance or very high-load query throughput, would usually be better served by a custom server.


Set Up the HTTP JSON API Service To Work With Highly Available Participants
***************************************************************************

If the participant node itself is configured to be highly available, depending on the setup you may want
to choose different approaches to connect to the passive participant node(s). In most setups, including those based on Canton,
you'll likely have an active participant node whose role can be taken over by a passive node in case the currently
active one drops. Just as for the *HTTP JSON API* itself, you can use orchestration systems or load balancers to
monitor the status of the participant nodes and have those point your (possibly highly-available) *HTTP JSON API*
nodes to the active participant node.

To learn how to run and monitor Canton with high availability, refer to the :ref:`Canton documentation <ha_arch>`.

