.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-nonempty-types-38464:

DA.NonEmpty.Types
=================

This module contains the type for non\-empty lists so we can give it a stable package id\.
This is reexported from DA\.NonEmpty so you should never need to import this module\.

Data Types
----------

.. _type-da-nonempty-types-nonempty-16010:

**data** `NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ a

  ``NonEmpty`` is the type of non\-empty lists\. In other words, it is the type of lists
  that always contain at least one element\. If ``x`` is a non\-empty list, you can obtain
  the first element with ``x.hd`` and the rest of the list with ``x.tl``\.

  .. _constr-da-nonempty-types-nonempty-68983:

  `NonEmpty <constr-da-nonempty-types-nonempty-68983_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - hd
         - a
         -
       * - tl
         - \[a\]
         -

  **instance** :ref:`Foldable <class-da-foldable-foldable-25994>` `NonEmpty <type-da-nonempty-types-nonempty-16010_>`_

  **instance** :ref:`Action <class-da-internal-prelude-action-68790>` `NonEmpty <type-da-nonempty-types-nonempty-16010_>`_

  **instance** :ref:`Applicative <class-da-internal-prelude-applicative-9257>` `NonEmpty <type-da-nonempty-types-nonempty-16010_>`_

  **instance** :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ a)

  **instance** :ref:`IsParties <class-da-internal-template-functions-isparties-53750>` (`NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ :ref:`Party <type-da-internal-lf-party-57932>`)

  **instance** :ref:`Traversable <class-da-traversable-traversable-18144>` `NonEmpty <type-da-nonempty-types-nonempty-16010_>`_

  **instance** Serializable a \=\> Serializable (`NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ a)

  **instance** :ref:`Functor <class-ghc-base-functor-31205>` `NonEmpty <type-da-nonempty-types-nonempty-16010_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ a)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ a)

  **instance** :ref:`Show <class-ghc-show-show-65360>` a \=\> :ref:`Show <class-ghc-show-show-65360>` (`NonEmpty <type-da-nonempty-types-nonempty-16010_>`_ a)
