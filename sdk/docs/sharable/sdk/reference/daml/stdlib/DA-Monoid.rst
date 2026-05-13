.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-monoid-95505:

DA.Monoid
=========

Data Types
----------

.. _type-da-monoid-types-all-38142:

**data** `All <type-da-monoid-types-all-38142_>`_

  Boolean monoid under conjunction (&&)

  .. _constr-da-monoid-types-all-18981:

  `All <constr-da-monoid-types-all-18981_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getAll
         - :ref:`Bool <type-ghc-types-bool-66265>`
         -

  **instance** :ref:`Monoid <class-da-internal-prelude-monoid-6742>` `All <type-da-monoid-types-all-38142_>`_

  **instance** :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` `All <type-da-monoid-types-all-38142_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `All <type-da-monoid-types-all-38142_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `All <type-da-monoid-types-all-38142_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `All <type-da-monoid-types-all-38142_>`_

.. _type-da-monoid-types-any-3989:

**data** `Any <type-da-monoid-types-any-3989_>`_

  Boolean Monoid under disjunction (||)

  .. _constr-da-monoid-types-any-54474:

  `Any <constr-da-monoid-types-any-54474_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getAny
         - :ref:`Bool <type-ghc-types-bool-66265>`
         -

  **instance** :ref:`Monoid <class-da-internal-prelude-monoid-6742>` `Any <type-da-monoid-types-any-3989_>`_

  **instance** :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` `Any <type-da-monoid-types-any-3989_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `Any <type-da-monoid-types-any-3989_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `Any <type-da-monoid-types-any-3989_>`_

  **instance** :ref:`Show <class-ghc-show-show-65360>` `Any <type-da-monoid-types-any-3989_>`_

.. _type-da-monoid-types-endo-95420:

**data** `Endo <type-da-monoid-types-endo-95420_>`_ a

  The monoid of endomorphisms under composition\.

  .. _constr-da-monoid-types-endo-7873:

  `Endo <constr-da-monoid-types-endo-7873_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - appEndo
         - a \-\> a
         -

  **instance** :ref:`Monoid <class-da-internal-prelude-monoid-6742>` (`Endo <type-da-monoid-types-endo-95420_>`_ a)

  **instance** :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Endo <type-da-monoid-types-endo-95420_>`_ a)

.. _type-da-monoid-types-product-66754:

**data** `Product <type-da-monoid-types-product-66754_>`_ a

  Monoid under (\*)

  .. code-block:: daml

    > Product 2 <> Product 3
    Product 6

  .. _constr-da-monoid-types-product-4241:

  `Product <constr-da-monoid-types-product-4241_>`_ a


  **instance** :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` a \=\> :ref:`Monoid <class-da-internal-prelude-monoid-6742>` (`Product <type-da-monoid-types-product-66754_>`_ a)

  **instance** :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` a \=\> :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Product <type-da-monoid-types-product-66754_>`_ a)

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Product <type-da-monoid-types-product-66754_>`_ a)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`Product <type-da-monoid-types-product-66754_>`_ a)

  **instance** :ref:`Additive <class-ghc-num-additive-25881>` a \=\> :ref:`Additive <class-ghc-num-additive-25881>` (`Product <type-da-monoid-types-product-66754_>`_ a)

  **instance** :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` a \=\> :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` (`Product <type-da-monoid-types-product-66754_>`_ a)

  **instance** :ref:`Show <class-ghc-show-show-65360>` a \=\> :ref:`Show <class-ghc-show-show-65360>` (`Product <type-da-monoid-types-product-66754_>`_ a)

.. _type-da-monoid-types-sum-76394:

**data** `Sum <type-da-monoid-types-sum-76394_>`_ a

  Monoid under (\+)

  .. code-block:: daml

    > Sum 1 <> Sum 2
    Sum 3

  .. _constr-da-monoid-types-sum-82289:

  `Sum <constr-da-monoid-types-sum-82289_>`_ a


  **instance** :ref:`Additive <class-ghc-num-additive-25881>` a \=\> :ref:`Monoid <class-da-internal-prelude-monoid-6742>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)

  **instance** :ref:`Additive <class-ghc-num-additive-25881>` a \=\> :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)

  **instance** :ref:`Additive <class-ghc-num-additive-25881>` a \=\> :ref:`Additive <class-ghc-num-additive-25881>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)

  **instance** :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` a \=\> :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)

  **instance** :ref:`Show <class-ghc-show-show-65360>` a \=\> :ref:`Show <class-ghc-show-show-65360>` (`Sum <type-da-monoid-types-sum-76394_>`_ a)
