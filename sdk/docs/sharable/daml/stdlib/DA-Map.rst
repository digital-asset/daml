.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-map-69618:

DA.Map
======

Note\: This is only supported in Daml\-LF 1\.11 or later\.

This module exports the generic map type ``Map k v`` and associated
functions\. This module should be imported qualified, for example\:

.. code-block:: daml

  import DA.Map (Map)
  import DA.Map qualified as M


This will give access to the ``Map`` type, and the various operations
as ``M.lookup``, ``M.insert``, ``M.fromList``, etc\.

``Map k v`` internally uses the built\-in order for the type ``k``\.
This means that keys that contain functions are not comparable
and will result in runtime errors\. To prevent this, the ``Ord k``
instance is required for most map operations\. It is recommended to
only use ``Map k v`` for key types that have an ``Ord k`` instance
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

Functions
---------

.. _function-da-map-fromlist-23400:

`fromList <function-da-map-fromlist-23400_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> \[(k, v)\] \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Create a map from a list of key/value pairs\.

.. _function-da-map-fromlistwithl-71603:

`fromListWithL <function-da-map-fromlistwithl-71603_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> v \-\> v) \-\> \[(k, v)\] \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Create a map from a list of key/value pairs with a combining
  function\. The combining function is only used when a key appears multiple
  times in the list and it takes two arguments\: the first one is the new value
  being inserted at that key and the second one is the value accumulated so
  far at that key\.
  Examples\:

  .. code-block:: daml

    >>> fromListWithL (++) [("A", [1]), ("A", [2]), ("B", [2]), ("B", [1]), ("A", [3])]
    fromList [("A", [3, 2, 1]), ("B", [1, 2])]
    >>> fromListWithL (++) [] == (empty : Map Text [Int])
    True

.. _function-da-map-fromlistwithr-67193:

`fromListWithR <function-da-map-fromlistwithr-67193_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> v \-\> v) \-\> \[(k, v)\] \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Create a map from a list of key/value pairs like ``fromListWithL``
  with the combining function flipped\. Examples\:

  .. code-block:: daml

    >>> fromListWithR (++) [("A", [1]), ("A", [2]), ("B", [2]), ("B", [1]), ("A", [3])]
    fromList [("A", [1, 2, 3]), ("B", [2, 1])]
    >>> fromListWithR (++) [] == (empty : Map Text [Int])
    True

.. _function-da-map-fromlistwith-28620:

`fromListWith <function-da-map-fromlistwith-28620_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> v \-\> v) \-\> \[(k, v)\] \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

.. _function-da-map-keys-97544:

`keys <function-da-map-keys-97544_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> \[k\]

  Get the list of keys in the map\. Keys are sorted according to the
  built\-in order for the type ``k``, which matches the ``Ord k`` instance
  when using ``deriving Ord``\.

  .. code-block:: daml

    >>> keys (fromList [("A", 1), ("C", 3), ("B", 2)])
    ["A", "B", "C"]

.. _function-da-map-values-1656:

`values <function-da-map-values-1656_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> \[v\]

  Get the list of values in the map\. These will be in the same order as
  their respective keys from ``M.keys``\.

  .. code-block:: daml

    >>> values (fromList [("A", 1), ("B", 2)])
    [1, 2]

.. _function-da-map-tolist-88193:

`toList <function-da-map-tolist-88193_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> \[(k, v)\]

  Convert the map to a list of key/value pairs\. These will be ordered
  by key, as in ``M.keys``\.

.. _function-da-map-empty-15540:

`empty <function-da-map-empty-15540_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k v

  The empty map\.

.. _function-da-map-size-29495:

`size <function-da-map-size-29495_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Int <type-ghc-types-int-37261>`

  Number of elements in the map\.

.. _function-da-map-null-81379:

`null <function-da-map-null-81379_>`_
  \: :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is the map empty?

.. _function-da-map-lookup-19256:

`lookup <function-da-map-lookup-19256_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` v

  Lookup the value at a key in the map\.

.. _function-da-map-member-48452:

`member <function-da-map-member-48452_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is the key a member of the map?

.. _function-da-map-filter-60004:

`filter <function-da-map-filter-60004_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Filter the ``Map`` using a predicate\: keep only the entries where the
  value satisfies the predicate\.

.. _function-da-map-filterwithkey-3168:

`filterWithKey <function-da-map-filterwithkey-3168_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (k \-\> v \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Filter the ``Map`` using a predicate\: keep only the entries which
  satisfy the predicate\.

.. _function-da-map-delete-97567:

`delete <function-da-map-delete-97567_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Delete a key and its value from the map\. When the key is not a
  member of the map, the original map is returned\.

.. _function-da-map-singleton-98784:

`singleton <function-da-map-singleton-98784_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Create a singleton map\.

.. _function-da-map-insert-53601:

`insert <function-da-map-insert-53601_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> k \-\> v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Insert a new key/value pair in the map\. If the key is already
  present in the map, the associated value is replaced with the
  supplied value\.

.. _function-da-map-insertwith-32465:

`insertWith <function-da-map-insertwith-32465_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> v \-\> v) \-\> k \-\> v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Insert a new key/value pair in the map\. If the key is already
  present in the map, it is combined with the previous value using the given function
  ``f new_value old_value``\.

.. _function-da-map-alter-84047:

`alter <function-da-map-alter-84047_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (:ref:`Optional <type-da-internal-prelude-optional-37153>` v \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` v) \-\> k \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  Update the value in ``m`` at ``k`` with ``f``, inserting or deleting as
  required\.  ``f`` will be called with either the value at ``k``, or ``None``
  if absent; ``f`` can return ``Some`` with a new value to be inserted in
  ``m`` (replacing the old value if there was one), or ``None`` to remove
  any ``k`` association ``m`` may have\.

  Some implications of this behavior\:

  alter identity k \= identity
  alter g k \. alter f k \= alter (g \. f) k
  alter (\_ \-\> Some v) k \= insert k v
  alter (\_ \-\> None) \= delete

.. _function-da-map-union-90078:

`union <function-da-map-union-90078_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  The union of two maps, preferring the first map when equal
  keys are encountered\.

.. _function-da-map-unionwith-55674:

`unionWith <function-da-map-unionwith-55674_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> v \-\> v) \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v \-\> :ref:`Map <type-da-internal-lf-map-90052>` k v

  The union of two maps using the combining function to merge values that
  exist in both maps\.

.. _function-da-map-merge-46179:

`merge <function-da-map-merge-46179_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (k \-\> a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` c) \-\> (k \-\> b \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` c) \-\> (k \-\> a \-\> b \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` c) \-\> :ref:`Map <type-da-internal-lf-map-90052>` k a \-\> :ref:`Map <type-da-internal-lf-map-90052>` k b \-\> :ref:`Map <type-da-internal-lf-map-90052>` k c

  Combine two maps, using separate functions based on whether
  a key appears only in the first map, only in the second map,
  or appears in both maps\.
