.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-textmap-81719:

DA.TextMap
==========

TextMap \- A map is an associative array data type composed of a
collection of key/value pairs such that, each possible key appears
at most once in the collection\.

Functions
---------

.. _function-da-textmap-fromlist-19033:

`fromList <function-da-textmap-fromlist-19033_>`_
  \: \[(:ref:`Text <type-ghc-types-text-51952>`, a)\] \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  Create a map from a list of key/value pairs\.

.. _function-da-textmap-fromlistwithl-22912:

`fromListWithL <function-da-textmap-fromlistwithl-22912_>`_
  \: (a \-\> a \-\> a) \-\> \[(:ref:`Text <type-ghc-types-text-51952>`, a)\] \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  Create a map from a list of key/value pairs with a combining
  function\. The combining function is only used when a key appears multiple
  times in the list and it takes two arguments\: the first one is the new value
  being inserted at that key and the second one is the value accumulated so
  far at that key\.
  Examples\:

  .. code-block:: daml

    >>> fromListWithL (++) [("A", [1]), ("A", [2]), ("B", [2]), ("B", [1]), ("A", [3])]
    fromList [("A", [3, 2, 1]), ("B", [1, 2])]
    >>> fromListWithL (++) [] == (empty : TextMap [Int])
    True

.. _function-da-textmap-fromlistwithr-69626:

`fromListWithR <function-da-textmap-fromlistwithr-69626_>`_
  \: (a \-\> a \-\> a) \-\> \[(:ref:`Text <type-ghc-types-text-51952>`, a)\] \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  Create a map from a list of key/value pairs like ``fromListWithL``
  with the combining function flipped\. Examples\:

  .. code-block:: daml

    >>> fromListWithR (++) [("A", [1]), ("A", [2]), ("B", [2]), ("B", [1]), ("A", [3])]
    fromList [("A", [1, 2, 3]), ("B", [2, 1])]
    >>> fromListWithR (++) [] == (empty : TextMap [Int])
    True

.. _function-da-textmap-fromlistwith-41741:

`fromListWith <function-da-textmap-fromlistwith-41741_>`_
  \: (a \-\> a \-\> a) \-\> \[(:ref:`Text <type-ghc-types-text-51952>`, a)\] \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

.. _function-da-textmap-tolist-95168:

`toList <function-da-textmap-tolist-95168_>`_
  \: :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> \[(:ref:`Text <type-ghc-types-text-51952>`, a)\]

  Convert the map to a list of key/value pairs where the keys are
  in ascending order\.

.. _function-da-textmap-empty-66187:

`empty <function-da-textmap-empty-66187_>`_
  \: :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  The empty map\.

.. _function-da-textmap-size-46150:

`size <function-da-textmap-size-46150_>`_
  \: :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`Int <type-ghc-types-int-37261>`

  Number of elements in the map\.

.. _function-da-textmap-null-64690:

`null <function-da-textmap-null-64690_>`_
  \: :ref:`TextMap <type-da-internal-lf-textmap-11691>` v \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is the map empty?

.. _function-da-textmap-lookup-87021:

`lookup <function-da-textmap-lookup-87021_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` a

  Lookup the value at a key in the map\.

.. _function-da-textmap-member-14417:

`member <function-da-textmap-member-14417_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Is the key a member of the map?

.. _function-da-textmap-filter-317:

`filter <function-da-textmap-filter-317_>`_
  \: (v \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v

  Filter the ``TextMap`` using a predicate\: keep only the entries where the
  value satisfies the predicate\.

.. _function-da-textmap-filterwithkey-64027:

`filterWithKey <function-da-textmap-filterwithkey-64027_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> v \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v

  Filter the ``TextMap`` using a predicate\: keep only the entries which
  satisfy the predicate\.

.. _function-da-textmap-delete-54270:

`delete <function-da-textmap-delete-54270_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  Delete a key and its value from the map\. When the key is not a
  member of the map, the original map is returned\.

.. _function-da-textmap-singleton-39431:

`singleton <function-da-textmap-singleton-39431_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  Create a singleton map\.

.. _function-da-textmap-insert-41312:

`insert <function-da-textmap-insert-41312_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  Insert a new key/value pair in the map\. If the key is already
  present in the map, the associated value is replaced with the
  supplied value\.

.. _function-da-textmap-insertwith-45464:

`insertWith <function-da-textmap-insertwith-45464_>`_
  \: (v \-\> v \-\> v) \-\> :ref:`Text <type-ghc-types-text-51952>` \-\> v \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` v

  Insert a new key/value pair in the map\. If the key is already
  present in the map, it is combined with the previous value using the given function
  ``f new_value old_value``\.

.. _function-da-textmap-union-13945:

`union <function-da-textmap-union-13945_>`_
  \: :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a

  The union of two maps, preferring the first map when equal
  keys are encountered\.

.. _function-da-textmap-merge-26784:

`merge <function-da-textmap-merge-26784_>`_
  \: (:ref:`Text <type-ghc-types-text-51952>` \-\> a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` c) \-\> (:ref:`Text <type-ghc-types-text-51952>` \-\> b \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` c) \-\> (:ref:`Text <type-ghc-types-text-51952>` \-\> a \-\> b \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` c) \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` a \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` b \-\> :ref:`TextMap <type-da-internal-lf-textmap-11691>` c

  Merge two maps\. ``merge f g h x y`` applies ``f`` to all key/value pairs
  whose key only appears in ``x``, ``g`` to all pairs whose key only appears
  in ``y`` and ``h`` to all pairs whose key appears in both ``x`` and ``y``\.
  In the end, all pairs yielding ``Some`` are collected as the result\.
