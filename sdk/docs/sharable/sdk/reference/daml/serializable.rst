.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Serializable
#######################

This page gives reference information on serializability in Daml.

Daml programs store data on the distributed ledger, for example contracts and choices.

Consider the following datatypes and template contract:

.. literalinclude:: code-snippets/Serializable.daml
   :language: daml
   :start-after: -- start implicit snippet
   :end-before: -- end implicit snippet

Explicit Serializable
---------------------

Stop automatically inferring serializability of data types.
This means data types used in fields of templates and choices will require explicit Serializable instances. \

See also :doc:`../smart-contract-upgrades`.
