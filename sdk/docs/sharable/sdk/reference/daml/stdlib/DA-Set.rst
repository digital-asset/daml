.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-set-6124:

DA.Set
======

Note\: This is only supported in Daml\-LF 1\.11 or later\.

This module exports the generic set type ``Set k`` and associated
functions\. This module should be imported qualified, for example\:

.. code-block:: daml

  import DA.Set (Set)
  import DA.Set qualified as S


This will give access to the ``Set`` type, and the various operations
as ``S.lookup``, ``S.insert``, ``S.fromList``, etc\.

``Set k`` internally uses the built\-in order for the type ``k``\.
This means that keys that contain functions are not comparable
and will result in runtime errors\. To prevent this, the ``Ord k``
instance is required for most set operations\. It is recommended to
only use ``Set k`` for key types that have an ``Ord k`` instance
that is derived automatically using ``deriving``\:

.. code-block:: daml

  data K = ...
    deriving (Eq, Ord)


This includes all built\-in types that aren't function types, such as
``Int``, ``Text``, ``Bool``, ``(a, b)`` assuming ``a`` and ``b`` have default
``Ord`` instances, ``Optional t`` and ``[t]`` assuming ``t`` has a
default ``Ord`` instance, ``Map k v`` assuming ``k`` and ``v`` have
default ``Ord`` instances, and ``Set k`` assuming ``k`` has a
default ``Ord`` instance\.

Data Types
----------

.. _type-da-set-types-set-90436:

**data** `Set <type-da-set-types-set-90436_>`_ k

  The type of a set\. This is a wrapper over the ``Map`` type\.

  .. _constr-da-set-types-set-78105:

  `Set <constr-da-set-types-set-78105_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - map
         - :ref:`Map <type-da-internal-lf-map-90052>` k ()
         -

  **instance** :ref:`Foldable <class-da-foldable-foldable-25994>` `Set <type-da-set-types-set-90436_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> :ref:`Monoid <class-da-internal-prelude-monoid-6742>` (`Set <type-da-set-types-set-90436_>`_ k)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Set <type-da-set-types-set-90436_>`_ k)

  **instance** :ref:`IsParties <class-da-internal-template-functions-isparties-53750>` (`Set <type-da-set-types-set-90436_>`_ :ref:`Party <type-da-internal-lf-party-57932>`)

  **instance** Serializable k \=\> Serializable (`Set <type-da-set-types-set-90436_>`_ k)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Set <type-da-set-types-set-90436_>`_ k)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`Set <type-da-set-types-set-90436_>`_ k)

  **instance** (:ref:`Ord <class-ghc-classes-ord-6395>` k, :ref:`Show <class-ghc-show-show-65360>` k) \=\> :ref:`Show <class-ghc-show-show-65360>` (`Set <type-da-set-types-set-90436_>`_ k)

Functions
---------

.. _function-da-set-empty-19742:

`empty <function-da-set-empty-19742_>`_
  \: `Set <type-da-set-types-set-90436_>`_ k

  The empty set\.

.. _function-da-set-size-6437:

`size <function-da-set-size-6437_>`_
  \: `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Int <type-ghc-types-int-37261>`

  The number of elements in the set\.

.. _function-da-set-tolist-26355:

`toList <function-da-set-tolist-26355_>`_
  \: `Set <type-da-set-types-set-90436_>`_ k \-\> \[k\]

  Convert the set to a list of elements\.

.. _function-da-set-fromlist-9190:

`fromList <function-da-set-fromlist-9190_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> \[k\] \-\> `Set <type-da-set-types-set-90436_>`_ k

  Create a set from a list of elements\.

.. _function-da-set-tomap-37614:

`toMap <function-da-set-tomap-37614_>`_
  \: `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Map <type-da-internal-lf-map-90052>` k ()

  Convert a ``Set`` into a ``Map``\.

.. _function-da-set-frommap-15501:

`fromMap <function-da-set-frommap-15501_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k () \-\> `Set <type-da-set-types-set-90436_>`_ k

  Create a ``Set`` from a ``Map``\.

.. _function-da-set-member-75542:

`member <function-da-set-member-75542_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is the element in the set?

.. _function-da-set-notmember-79044:

`notMember <function-da-set-notmember-79044_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is the element not in the set?
  ``notMember k s`` is equivalent to ``not (member k s)``\.

.. _function-da-set-null-99389:

`null <function-da-set-null-99389_>`_
  \: `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is this the empty set?

.. _function-da-set-insert-58479:

`insert <function-da-set-insert-58479_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k

  Insert an element in a set\. If the set already contains the
  element, this returns the set unchanged\.

.. _function-da-set-filter-76182:

`filter <function-da-set-filter-76182_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (k \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k

  Filter all elements that satisfy the predicate\.

.. _function-da-set-delete-52281:

`delete <function-da-set-delete-52281_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k

  Delete an element from a set\.

.. _function-da-set-singleton-15574:

`singleton <function-da-set-singleton-15574_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> `Set <type-da-set-types-set-90436_>`_ k

  Create a singleton set\.

.. _function-da-set-union-79876:

`union <function-da-set-union-79876_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k

  The union of two sets\.

.. _function-da-set-intersection-70017:

`intersection <function-da-set-intersection-70017_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k

  The intersection of two sets\.

.. _function-da-set-difference-68545:

`difference <function-da-set-difference-68545_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k

  ``difference x y`` returns the set consisting of all
  elements in ``x`` that are not in ``y``\.

  > > > fromList \[1, 2, 3\] `difference` fromList \[1, 4\]
  > > > fromList \[2, 3\]

.. _function-da-set-issubsetof-34493:

`isSubsetOf <function-da-set-issubsetof-34493_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isSubsetOf a b`` returns true if ``a`` is a subset of ``b``,
  that is, if every element of ``a`` is in ``b``\.

.. _function-da-set-ispropersubsetof-90093:

`isProperSubsetOf <function-da-set-ispropersubsetof-90093_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> `Set <type-da-set-types-set-90436_>`_ k \-\> `Set <type-da-set-types-set-90436_>`_ k \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  ``isProperSubsetOf a b`` returns true if ``a`` is a proper subset of ``b``\.
  That is, if ``a`` is a subset of ``b`` but not equal to ``b``\.
