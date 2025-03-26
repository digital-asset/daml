.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-list-builtinorder-49213:

DA.List.BuiltinOrder
====================

Note\: This is only supported in Daml\-LF 1\.11 or later\.

This module provides variants of other standard library
functions that are based on the builtin Daml\-LF ordering rather
than user\-defined ordering\. This is the same order also used
by ``DA.Map``\.

These functions are usually much more efficient than their
``Ord``\-based counterparts\.

Note that the functions in this module still require ``Ord``
constraints\. This is purely to enforce that you don’t
pass in values that cannot be compared, e\.g\., functions\. The
implementation of those instances is not used\.

Functions
---------

.. _function-da-list-builtinorder-dedup-38418:

`dedup <function-da-list-builtinorder-dedup-38418_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> \[a\]

  ``dedup l`` removes duplicate elements from a list\. In particular,
  it keeps only the first occurrence of each element\.

  ``dedup`` is stable so the elements in the output are ordered
  by their first occurrence in the input\. If you do not need
  stability, consider using ``dedupSort`` which is more efficient\.

  .. code-block:: daml

    >>> dedup [3, 1, 1, 3]
    [3, 1]

.. _function-da-list-builtinorder-dedupon-23739:

`dedupOn <function-da-list-builtinorder-dedupon-23739_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> k) \-\> \[v\] \-\> \[v\]

  A version of ``dedup`` where deduplication is done
  after applying the given function\. Example use\: ``dedupOn (.employeeNo) employees``\.

  ``dedupOn`` is stable so the elements in the output are ordered
  by their first occurrence in the input\. If you do not need
  stability, consider using ``dedupOnSort`` which is more efficient\.

  .. code-block:: daml

    >>> dedupOn fst [(3, "a"), (1, "b"), (1, "c"), (3, "d")]
    [(3, "a"), (1, "b")]

.. _function-da-list-builtinorder-dedupsort-5846:

`dedupSort <function-da-list-builtinorder-dedupsort-5846_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> \[a\]

  ``dedupSort`` is a more efficient variant of ``dedup``
  that does not preserve the order of the input elements\.
  Instead the output will be sorted acoording to the builtin Daml\-LF
  ordering\.

  .. code-block:: daml

    >>> dedupSort [3, 1, 1, 3]
    [1, 3]

.. _function-da-list-builtinorder-deduponsort-69087:

`dedupOnSort <function-da-list-builtinorder-deduponsort-69087_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (v \-\> k) \-\> \[v\] \-\> \[v\]

  ``dedupOnSort`` is a more efficient variant of ``dedupOn``
  that does not preserve the order of the input elements\.
  Instead the output will be sorted on the values returned by the function\.

  For duplicates, the first element in the list will be included in the output\.

  .. code-block:: daml

    >>> dedupOnSort fst [(3, "a"), (1, "b"), (1, "c"), (3, "d")]
    [(1, "b"), (3, "a")]

.. _function-da-list-builtinorder-sort-65819:

`sort <function-da-list-builtinorder-sort-65819_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> \[a\]

  Sort the list according to the Daml\-LF ordering\.

  Values that are identical according to the builtin Daml\-LF ordering
  are indistinguishable so stability is not relevant here\.

  .. code-block:: daml

    >>> sort [3,1,2]
    [1,2,3]

.. _function-da-list-builtinorder-sorton-7978:

`sortOn <function-da-list-builtinorder-sorton-7978_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` b \=\> (a \-\> b) \-\> \[a\] \-\> \[a\]

  ``sortOn f`` is a version of sort that allows sorting
  on the result of the given function\.

  ``sortOn`` is stable so elements that map to the same sort key
  will be ordered by their position in the input\.

  .. code-block:: daml

    >>> sortOn fst [(3, "a"), (1, "b"), (3, "c"), (2, "d")]
    [(1, "b"), (2, "d"), (3, "a"), (3, "c")]

.. _function-da-list-builtinorder-unique-2492:

`unique <function-da-list-builtinorder-unique-2492_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Returns True if and only if there are no duplicate elements in the given list\.

  .. code-block:: daml

    >>> unique [1, 2, 3]
    True

.. _function-da-list-builtinorder-uniqueon-93017:

`uniqueOn <function-da-list-builtinorder-uniqueon-93017_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (a \-\> k) \-\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Returns True if and only if there are no duplicate elements in the given list
  after applyng function\.

  .. code-block:: daml

    >>> uniqueOn fst [(1, 2), (2, 42), (1, 3)]
    False
