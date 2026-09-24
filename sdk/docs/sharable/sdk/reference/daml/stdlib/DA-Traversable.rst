.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-traversable-75075:

DA.Traversable
==============

Class of data structures that can be traversed from left to right, performing an action on each element\.
You typically would want to import this module qualified to avoid clashes with
functions defined in ``Prelude``\. Ie\.\:

.. code-block:: daml

  import DA.Traversable   qualified as F

Typeclasses
-----------

.. _class-da-traversable-traversable-18144:

**class** (:ref:`Functor <class-ghc-base-functor-31205>` t, :ref:`Foldable <class-da-foldable-foldable-25994>` t) \=\> `Traversable <class-da-traversable-traversable-18144_>`_ t **where**

  Functors representing data structures that can be traversed from left to right\.

  .. _function-da-traversable-mapa-2898:

  `mapA <function-da-traversable-mapa-2898_>`_
    \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> (a \-\> f b) \-\> t a \-\> f (t b)

    Map each element of a structure to an action, evaluate these actions
    from left to right, and collect the results\.

  .. _function-da-traversable-sequence-31922:

  `sequence <function-da-traversable-sequence-31922_>`_
    \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> t (f a) \-\> f (t a)

    Evaluate each action in the structure from left to right, and
    collect the results\.

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Traversable <class-da-traversable-traversable-18144_>`_ (:ref:`Map <type-da-internal-lf-map-90052>` k)

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ :ref:`TextMap <type-da-internal-lf-textmap-11691>`

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ :ref:`Optional <type-da-internal-prelude-optional-37153>`

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>`

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ (:ref:`Validation <type-da-validation-types-validation-39644>` err)

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ (:ref:`Either <type-da-types-either-56020>` a)

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ :ref:`([]) <type-ghc-types-x-2599>`

  **instance** `Traversable <class-da-traversable-traversable-18144_>`_ a

Functions
---------

.. _function-da-traversable-fora-19271:

`forA <function-da-traversable-fora-19271_>`_
  \: (`Traversable <class-da-traversable-traversable-18144_>`_ t, :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f) \=\> t a \-\> (a \-\> f b) \-\> f (t b)

  ``forA`` is ``mapA`` with its arguments flipped\.
