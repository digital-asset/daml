.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Serializable
#######################

.. TODO: This page leans more towards a how-to than a reference in tone.
    Consider moving it and leaving a minimal reference here.

This page gives reference information on serializability in Daml.

Daml programs store data on the distributed ledger, for example contracts and choices.

Consider the template contract and auxiliary datatype ``InstrumentId``:

.. literalinclude:: code-snippets/ImplicitSerializable.daml
   :language: daml
   :start-after: -- start implicit serializable snippet
   :end-before: -- end implicit serializable snippet

The Daml compiler will `infer` that it is safe to include ``InstrumentId`` in a template contract,
since it does not contain any inherently unserializable types (e.g. functions).

When upgrading the package containing these datatypes,
we must ensure that we can still read serialized data that was stored in the past.
Therefore, :doc:`Smart Contract Upgrades <../smart-contract-upgrades>` will check that these serializable datatypes are only changed in a backwards-compatible fashion.
Concretely this means that only optional fields may be added.

This constraint also applies to data types which can be serialized,
but that are never actually stored on the ledger, such as ``AssetSummary`` in the example.

Explicit Serializable
---------------------

As we mentioned in the previous section, implicitly inferring serializability is problematic for helper types.

These data types, typically used in-memory during complex calculations,
but not directly referenced in templates or choices,
are also subject to the strict :doc:`Smart Contract Upgrade <../smart-contract-upgrades>` checks.

Therefore, we recommend Daml developers to enable the ``--explicit-serializable=yes`` option in ``build-options`` in ``daml.yaml``.

This stops the compiler from automatically inferring the serializability of data types.
Instead, an explicit ``Serializable`` instance must be derived,
and this can be omitted on helper data types that are only used in-memory:

.. literalinclude:: code-snippets/ExplicitSerializable.daml
   :language: daml
   :start-after: -- start explicit serializable snippet
   :end-before: -- end explicit serializable snippet

While this requires a bit more typing, we recommend turning this on,
since it forces Daml developers to think about which data types should end up on the ledger,
and which ones should be upgradeable.

Explicit Serializable will become the default in a future release,
so users are encouraged to opt-in early.
