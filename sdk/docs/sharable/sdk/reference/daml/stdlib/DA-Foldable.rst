.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-foldable-94882:

DA.Foldable
===========

Class of data structures that can be folded to a summary value\.
It's a good idea to import this module qualified to avoid clashes with
functions defined in ``Prelude``\. Ie\.\:

.. code-block:: daml

  import DA.Foldable qualified as F

Typeclasses
-----------

.. _class-da-foldable-foldable-25994:

**class** `Foldable <class-da-foldable-foldable-25994_>`_ t **where**

  Class of data structures that can be folded to a summary value\.

  .. _function-da-foldable-fold-64569:

  `fold <function-da-foldable-fold-64569_>`_
    \: :ref:`Monoid <class-da-internal-prelude-monoid-6742>` m \=\> t m \-\> m

    Combine the elements of a structure using a monoid\.

  .. _function-da-foldable-foldmap-73376:

  `foldMap <function-da-foldable-foldmap-73376_>`_
    \: :ref:`Monoid <class-da-internal-prelude-monoid-6742>` m \=\> (a \-\> m) \-\> t a \-\> m

    Combine the elements of a structure using a monoid\.

  .. _function-da-foldable-foldr-33982:

  `foldr <function-da-foldable-foldr-33982_>`_
    \: (a \-\> b \-\> b) \-\> b \-\> t a \-\> b

    Right\-associative fold of a structure\.

  .. _function-da-foldable-foldl-16220:

  `foldl <function-da-foldable-foldl-16220_>`_
    \: (b \-\> a \-\> b) \-\> b \-\> t a \-\> b

    Left\-associative fold of a structure\.

  .. _function-da-foldable-foldr1-73556:

  `foldr1 <function-da-foldable-foldr1-73556_>`_
    \: (a \-\> a \-\> a) \-\> t a \-\> a

    A variant of foldr that has no base case, and thus should only be applied to non\-empty structures\.

  .. _function-da-foldable-foldl1-10610:

  `foldl1 <function-da-foldable-foldl1-10610_>`_
    \: (a \-\> a \-\> a) \-\> t a \-\> a

    A variant of foldl that has no base case, and thus should only be applied to non\-empty structures\.

  .. _function-da-foldable-tolist-58625:

  `toList <function-da-foldable-tolist-58625_>`_
    \: t a \-\> \[a\]

    List of elements of a structure, from left to right\.

  .. _function-da-foldable-null-48579:

  `null <function-da-foldable-null-48579_>`_
    \: t a \-\> :ref:`Bool <type-ghc-types-bool-66265>`

    Test whether the structure is empty\. The default implementation is optimized for structures that are similar to cons\-lists, because there is no general way to do better\.

  .. _function-da-foldable-length-39462:

  `length <function-da-foldable-length-39462_>`_
    \: t a \-\> :ref:`Int <type-ghc-types-int-37261>`

    Returns the size/length of a finite structure as an ``Int``\. The default implementation is optimized for structures that are similar to cons\-lists, because there is no general way to do better\.

  .. _function-da-foldable-elem-30373:

  `elem <function-da-foldable-elem-30373_>`_
    \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> a \-\> t a \-\> :ref:`Bool <type-ghc-types-bool-66265>`

    Does the element occur in the structure?

  .. _function-da-foldable-sum-87024:

  `sum <function-da-foldable-sum-87024_>`_
    \: :ref:`Additive <class-ghc-num-additive-25881>` a \=\> t a \-\> a

    The sum function computes the sum of the numbers of a structure\.

  .. _function-da-foldable-product-30784:

  `product <function-da-foldable-product-30784_>`_
    \: :ref:`Multiplicative <class-ghc-num-multiplicative-10593>` a \=\> t a \-\> a

    The product function computes the product of the numbers of a structure\.

  .. _function-da-foldable-minimum-4521:

  `minimum <function-da-foldable-minimum-4521_>`_
    \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> t a \-\> a

    The least element of a non\-empty structure\.

  .. _function-da-foldable-maximum-18675:

  `maximum <function-da-foldable-maximum-18675_>`_
    \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> t a \-\> a

    The largest element of a non\-empty structure\.

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Foldable <class-da-foldable-foldable-25994_>`_ (:ref:`Map <type-da-internal-lf-map-90052>` k)

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ :ref:`Optional <type-da-internal-prelude-optional-37153>`

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>`

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ :ref:`Set <type-da-set-types-set-90436>`

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ (:ref:`Validation <type-da-validation-types-validation-39644>` err)

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ (:ref:`Either <type-da-types-either-56020>` a)

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ :ref:`([]) <type-ghc-types-x-2599>`

  **instance** `Foldable <class-da-foldable-foldable-25994_>`_ a

Functions
---------

.. _function-da-foldable-mapa-78745:

`mapA_ <function-da-foldable-mapa-78745_>`_
  \: (`Foldable <class-da-foldable-foldable-25994_>`_ t, :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f) \=\> (a \-\> f b) \-\> t a \-\> f ()

  Map each element of a structure to an action, evaluate these
  actions from left to right, and ignore the results\. For a version
  that doesn't ignore the results see 'DA\.Traversable\.mapA'\.

.. _function-da-foldable-fora-54422:

`forA_ <function-da-foldable-fora-54422_>`_
  \: (`Foldable <class-da-foldable-foldable-25994_>`_ t, :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f) \=\> t a \-\> (a \-\> f b) \-\> f ()

  'for\_' is 'mapA\_' with its arguments flipped\. For a version
  that doesn't ignore the results see 'DA\.Traversable\.forA'\.

.. _function-da-foldable-form-34370:

`forM_ <function-da-foldable-form-34370_>`_
  \: (`Foldable <class-da-foldable-foldable-25994_>`_ t, :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f) \=\> t a \-\> (a \-\> f b) \-\> f ()

.. _function-da-foldable-sequence-26917:

`sequence_ <function-da-foldable-sequence-26917_>`_
  \: (`Foldable <class-da-foldable-foldable-25994_>`_ t, :ref:`Action <class-da-internal-prelude-action-68790>` m) \=\> t (m a) \-\> m ()

  Evaluate each action in the structure from left to right,
  and ignore the results\. For a version that doesn't ignore the
  results see 'DA\.Traversable\.sequence'\.

.. _function-da-foldable-concat-71538:

`concat <function-da-foldable-concat-71538_>`_
  \: `Foldable <class-da-foldable-foldable-25994_>`_ t \=\> t \[a\] \-\> \[a\]

  The concatenation of all the elements of a container of lists\.

.. _function-da-foldable-and-52214:

`and <function-da-foldable-and-52214_>`_
  \: `Foldable <class-da-foldable-foldable-25994_>`_ t \=\> t :ref:`Bool <type-ghc-types-bool-66265>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``and`` returns the conjunction of a container of Bools\. For the result to be ``True``, the container must be finite; ``False``, however, results from a ``False`` value finitely far from the left end\.

.. _function-da-foldable-or-15333:

`or <function-da-foldable-or-15333_>`_
  \: `Foldable <class-da-foldable-foldable-25994_>`_ t \=\> t :ref:`Bool <type-ghc-types-bool-66265>` \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``or`` returns the disjunction of a container of Bools\. For the result to be ``False``, the container must be finite; ``True``, however, results from a ``True`` value finitely far from the left end\.

.. _function-da-foldable-any-93587:

`any <function-da-foldable-any-93587_>`_
  \: `Foldable <class-da-foldable-foldable-25994_>`_ t \=\> (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> t a \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Determines whether any element of the structure satisfies the predicate\.

.. _function-da-foldable-all-59560:

`all <function-da-foldable-all-59560_>`_
  \: `Foldable <class-da-foldable-foldable-25994_>`_ t \=\> (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> t a \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Determines whether all elements of the structure satisfy the predicate\.
