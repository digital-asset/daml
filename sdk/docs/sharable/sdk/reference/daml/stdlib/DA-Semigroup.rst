.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-semigroup-27147:

DA.Semigroup
============

Data Types
----------

.. _type-da-semigroup-types-max-52699:

**data** `Max <type-da-semigroup-types-max-52699_>`_ a

  Semigroup under ``max``

  .. code-block:: daml

    > Max 23 <> Max 42
    Max 42

  .. _constr-da-semigroup-types-max-20326:

  `Max <constr-da-semigroup-types-max-20326_>`_ a


  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Max <type-da-semigroup-types-max-52699_>`_ a)

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Max <type-da-semigroup-types-max-52699_>`_ a)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`Max <type-da-semigroup-types-max-52699_>`_ a)

  **instance** :ref:`Show <class-ghc-show-show-65360>` a \=\> :ref:`Show <class-ghc-show-show-65360>` (`Max <type-da-semigroup-types-max-52699_>`_ a)

.. _type-da-semigroup-types-min-78217:

**data** `Min <type-da-semigroup-types-min-78217_>`_ a

  Semigroup under ``min``

  .. code-block:: daml

    > Min 23 <> Min 42
    Min 23

  .. _constr-da-semigroup-types-min-6532:

  `Min <constr-da-semigroup-types-min-6532_>`_ a


  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Min <type-da-semigroup-types-min-78217_>`_ a)

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Min <type-da-semigroup-types-min-78217_>`_ a)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`Min <type-da-semigroup-types-min-78217_>`_ a)

  **instance** :ref:`Show <class-ghc-show-show-65360>` a \=\> :ref:`Show <class-ghc-show-show-65360>` (`Min <type-da-semigroup-types-min-78217_>`_ a)
