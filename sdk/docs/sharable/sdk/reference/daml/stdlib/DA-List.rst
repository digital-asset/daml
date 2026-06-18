.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-list-85985:

DA.List
=======

List

Functions
---------

.. _function-da-list-sort-96399:

`sort <function-da-list-sort-96399_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> \[a\]

  The ``sort`` function implements a stable sorting algorithm\. It is
  a special case of ``sortBy``, which allows the programmer to supply
  their own comparison function\.

  Elements are arranged from lowest to highest, keeping duplicates in
  the order they appeared in the input (a stable sort)\.

.. _function-da-list-sortby-71202:

`sortBy <function-da-list-sortby-71202_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> \[a\]

  The ``sortBy`` function is the non\-overloaded version of ``sort``\.

.. _function-da-list-minimumby-84625:

`minimumBy <function-da-list-minimumby-84625_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> a

  ``minimumBy f xs`` returns the first element ``x`` of ``xs`` for which ``f x y``
  is either ``LT`` or ``EQ`` for all other ``y`` in ``xs``\. ``xs`` must be non\-empty\.

.. _function-da-list-maximumby-22187:

`maximumBy <function-da-list-maximumby-22187_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> a

  ``maximumBy f xs`` returns the first element ``x`` of ``xs`` for which ``f x y``
  is either ``GT`` or ``EQ`` for all other ``y`` in ``xs``\. ``xs`` must be non\-empty\.

.. _function-da-list-sorton-99758:

`sortOn <function-da-list-sorton-99758_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (a \-\> k) \-\> \[a\] \-\> \[a\]

  Sort a list by comparing the results of a key function applied to
  each element\. ``sortOn f`` is equivalent to ``sortBy (comparing f)``,
  but has the performance advantage of only evaluating ``f`` once for
  each element in the input list\. This is sometimes called the
  decorate\-sort\-undecorate paradigm\.

  Elements are arranged from from lowest to highest, keeping
  duplicates in the order they appeared in the input\.

.. _function-da-list-minimumon-90785:

`minimumOn <function-da-list-minimumon-90785_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (a \-\> k) \-\> \[a\] \-\> a

  ``minimumOn f xs`` returns the first element ``x`` of ``xs`` for which ``f x``
  is smaller than or equal to any other ``f y`` for ``y`` in ``xs``\. ``xs`` must be
  non\-empty\.

.. _function-da-list-maximumon-98335:

`maximumOn <function-da-list-maximumon-98335_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (a \-\> k) \-\> \[a\] \-\> a

  ``maximumOn f xs`` returns the first element ``x`` of ``xs`` for which ``f x``
  is greater than or equal to any other ``f y`` for ``y`` in ``xs``\. ``xs`` must be
  non\-empty\.

.. _function-da-list-mergeby-31951:

`mergeBy <function-da-list-mergeby-31951_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> \[a\] \-\> \[a\]

  Merge two sorted lists using into a single, sorted whole, allowing
  the programmer to specify the comparison function\.

.. _function-da-list-combinepairs-8661:

`combinePairs <function-da-list-combinepairs-8661_>`_
  \: (a \-\> a \-\> a) \-\> \[a\] \-\> \[a\]

  Combine elements pairwise by means of a programmer supplied
  function from two list inputs into a single list\.

.. _function-da-list-foldbalanced1-46720:

`foldBalanced1 <function-da-list-foldbalanced1-46720_>`_
  \: (a \-\> a \-\> a) \-\> \[a\] \-\> a

  Fold a non\-empty list in a balanced way\. Balanced means that each
  element has approximately the same depth in the operator
  tree\. Approximately the same depth means that the difference
  between maximum and minimum depth is at most 1\. The accumulation
  operation must be associative and commutative in order to get the
  same result as ``foldl1`` or ``foldr1``\.

.. _function-da-list-group-62411:

`group <function-da-list-group-62411_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[\[a\]\]

  The 'group' function groups equal elements into sublists such
  that the concatenation of the result is equal to the argument\.

.. _function-da-list-groupby-62666:

`groupBy <function-da-list-groupby-62666_>`_
  \: (a \-\> a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> \[\[a\]\]

  The 'groupBy' function is the non\-overloaded version of 'group'\.

.. _function-da-list-groupon-4918:

`groupOn <function-da-list-groupon-4918_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` k \=\> (a \-\> k) \-\> \[a\] \-\> \[\[a\]\]

  Similar to 'group', except that the equality is done on an
  extracted value\.

.. _function-da-list-dedup-27230:

`dedup <function-da-list-dedup-27230_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> \[a\]

  ``dedup l`` removes duplicate elements from a list\. In particular,
  it keeps only the first occurrence of each element\. It is a
  special case of ``dedupBy``, which allows the programmer to supply
  their own equality test\.
  ``dedup`` is called ``nub`` in Haskell\.

.. _function-da-list-dedupby-29335:

`dedupBy <function-da-list-dedupby-29335_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> \[a\]

  A version of ``dedup`` with a custom predicate\.

.. _function-da-list-dedupon-81495:

`dedupOn <function-da-list-dedupon-81495_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (a \-\> k) \-\> \[a\] \-\> \[a\]

  A version of ``dedup`` where deduplication is done
  after applyng function\. Example use\: ``dedupOn (.employeeNo) employees``

.. _function-da-list-dedupsort-78698:

`dedupSort <function-da-list-dedupsort-78698_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> \[a\]

  The ``dedupSort`` function sorts and removes duplicate elements from a
  list\. In particular, it keeps only the first occurrence of each
  element\.

.. _function-da-list-dedupsortby-97595:

`dedupSortBy <function-da-list-dedupsortby-97595_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> \[a\]

  A version of ``dedupSort`` with a custom predicate\.

.. _function-da-list-unique-23008:

`unique <function-da-list-unique-23008_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` a \=\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Returns True if and only if there are no duplicate elements in the given list\.

.. _function-da-list-uniqueby-86149:

`uniqueBy <function-da-list-uniqueby-86149_>`_
  \: (a \-\> a \-\> :ref:`Ordering <type-ghc-types-ordering-35353>`) \-\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  A version of ``unique`` with a custom predicate\.

.. _function-da-list-uniqueon-39349:

`uniqueOn <function-da-list-uniqueon-39349_>`_
  \: :ref:`Ord <class-ghc-classes-ord-6395>` k \=\> (a \-\> k) \-\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Returns True if and only if there are no duplicate elements in the given list
  after applyng function\. Example use\: ``assert $ uniqueOn (.employeeNo) employees``

.. _function-da-list-replace-72492:

`replace <function-da-list-replace-72492_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> \[a\] \-\> \[a\]

  Given a list and a replacement list, replaces each occurance of
  the search list with the replacement list in the operation list\.

.. _function-da-list-dropprefix-26566:

`dropPrefix <function-da-list-dropprefix-26566_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> \[a\]

  Drops the given prefix from a list\. It returns the original
  sequence if the sequence doesn't start with the given prefix\.

.. _function-da-list-dropsuffix-41813:

`dropSuffix <function-da-list-dropsuffix-41813_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> \[a\]

  Drops the given suffix from a list\. It returns the original
  sequence if the sequence doesn't end with the given suffix\.

.. _function-da-list-stripprefix-65866:

`stripPrefix <function-da-list-stripprefix-65866_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` \[a\]

  The ``stripPrefix`` function drops the given prefix from a list\.
  It returns ``None`` if the list did not start with the prefix
  given, or ``Some`` the list after the prefix, if it does\.

.. _function-da-list-stripsuffix-23153:

`stripSuffix <function-da-list-stripsuffix-23153_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` \[a\]

  Return the prefix of the second list if its suffix matches the
  entire first list\.

.. _function-da-list-stripinfix-68205:

`stripInfix <function-da-list-stripinfix-68205_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` (\[a\], \[a\])

  Return the string before and after the search string or ``None``
  if the search string is not found\.

  .. code-block:: daml

    >>> stripInfix [0,0] [1,0,0,2,0,0,3]
    Some ([1], [2,0,0,3])

    >>> stripInfix [0,0] [1,2,0,4,5]
    None

.. _function-da-list-isprefixof-27346:

`isPrefixOf <function-da-list-isprefixof-27346_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isPrefixOf`` function takes two lists and returns ``True`` if
  and only if the first is a prefix of the second\.

.. _function-da-list-issuffixof-26645:

`isSuffixOf <function-da-list-issuffixof-26645_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isSuffixOf`` function takes two lists and returns ``True`` if
  and only if the first list is a suffix of the second\.

.. _function-da-list-isinfixof-7159:

`isInfixOf <function-da-list-isinfixof-7159_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isInfixOf`` function takes two lists and returns ``True`` if
  and only if the first list is contained anywhere within the second\.

.. _function-da-list-mapaccuml-18387:

`mapAccumL <function-da-list-mapaccuml-18387_>`_
  \: (acc \-\> x \-\> (acc, y)) \-\> acc \-\> \[x\] \-\> (acc, \[y\])

  The ``mapAccumL`` function combines the behaviours of ``map`` and
  ``foldl``; it applies a function to each element of a list, passing
  an accumulating parameter from left to right, and returning a final
  value of this accumulator together with the new list\.

.. _function-da-list-mapwithindex-75685:

`mapWithIndex <function-da-list-mapwithindex-75685_>`_
  \: (:ref:`Int <type-ghc-types-int-37261>` \-\> a \-\> b) \-\> \[a\] \-\> \[b\]

  A generalisation of ``map``, ``mapWithIndex`` takes a mapping
  function that also depends on the element's index, and applies it to every
  element in the sequence\.

.. _function-da-list-inits-75071:

`inits <function-da-list-inits-75071_>`_
  \: \[a\] \-\> \[\[a\]\]

  The ``inits`` function returns all initial segments of the argument,
  shortest first\.

.. _function-da-list-intersperse-31314:

`intersperse <function-da-list-intersperse-31314_>`_
  \: a \-\> \[a\] \-\> \[a\]

  The ``intersperse`` function takes an element and a list and
  \"intersperses\" that element between the elements of the list\.

.. _function-da-list-intercalate-85238:

`intercalate <function-da-list-intercalate-85238_>`_
  \: \[a\] \-\> \[\[a\]\] \-\> \[a\]

  ``intercalate`` inserts the list ``xs`` in between the lists in ``xss``
  and concatenates the result\.

.. _function-da-list-tails-57647:

`tails <function-da-list-tails-57647_>`_
  \: \[a\] \-\> \[\[a\]\]

  The ``tails`` function returns all final segments of the argument,
  longest first\.

.. _function-da-list-dropwhileend-92606:

`dropWhileEnd <function-da-list-dropwhileend-92606_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> \[a\]

  A version of ``dropWhile`` operating from the end\.

.. _function-da-list-takewhileend-40268:

`takeWhileEnd <function-da-list-takewhileend-40268_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> \[a\]

  A version of ``takeWhile`` operating from the end\.

.. _function-da-list-transpose-84171:

`transpose <function-da-list-transpose-84171_>`_
  \: \[\[a\]\] \-\> \[\[a\]\]

  The ``transpose`` function transposes the rows and columns of its
  argument\.

.. _function-da-list-breakend-41551:

`breakEnd <function-da-list-breakend-41551_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> (\[a\], \[a\])

  Break, but from the end\.

.. _function-da-list-breakon-39026:

`breakOn <function-da-list-breakon-39026_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> (\[a\], \[a\])

  Find the first instance of ``needle`` in ``haystack``\.
  The first element of the returned tuple is the prefix of ``haystack``
  before ``needle`` is matched\. The second is the remainder of
  ``haystack``, starting with the match\. If you want the remainder
  *without* the match, use ``stripInfix``\.

.. _function-da-list-breakonend-30980:

`breakOnEnd <function-da-list-breakonend-30980_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> (\[a\], \[a\])

  Similar to ``breakOn``, but searches from the end of the
  string\.

  The first element of the returned tuple is the prefix of ``haystack``
  up to and including the last match of ``needle``\. The second is the
  remainder of ``haystack``, following the match\.

.. _function-da-list-linesby-68954:

`linesBy <function-da-list-linesby-68954_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> \[\[a\]\]

  A variant of ``lines`` with a custom test\. In particular, if there
  is a trailing separator it will be discarded\.

.. _function-da-list-wordsby-74460:

`wordsBy <function-da-list-wordsby-74460_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> \[\[a\]\]

  A variant of ``words`` with a custom test\. In particular, adjacent
  separators are discarded, as are leading or trailing separators\.

.. _function-da-list-head-92101:

`head <function-da-list-head-92101_>`_
  \: \[a\] \-\> a

  Extract the first element of a list, which must be non\-empty\.

.. _function-da-list-tail-16805:

`tail <function-da-list-tail-16805_>`_
  \: \[a\] \-\> \[a\]

  Extract the elements after the head of a list, which must be
  non\-empty\.

.. _function-da-list-last-56071:

`last <function-da-list-last-56071_>`_
  \: \[a\] \-\> a

  Extract the last element of a list, which must be finite and
  non\-empty\.

.. _function-da-list-init-2389:

`init <function-da-list-init-2389_>`_
  \: \[a\] \-\> \[a\]

  Return all the elements of a list except the last one\. The list
  must be non\-empty\.

.. _function-da-list-foldl1-60813:

`foldl1 <function-da-list-foldl1-60813_>`_
  \: (a \-\> a \-\> a) \-\> \[a\] \-\> a

  Left associative fold of a list that must be non\-empty\.

.. _function-da-list-foldr1-23463:

`foldr1 <function-da-list-foldr1-23463_>`_
  \: (a \-\> a \-\> a) \-\> \[a\] \-\> a

  Right associative fold of a list that must be non\-empty\.

.. _function-da-list-repeatedly-9930:

`repeatedly <function-da-list-repeatedly-9930_>`_
  \: (\[a\] \-\> (b, \[a\])) \-\> \[a\] \-\> \[b\]

  Apply some operation repeatedly, producing an element of output
  and the remainder of the list\.

.. _function-da-list-chunksof-64138:

`chunksOf <function-da-list-chunksof-64138_>`_
  \: :ref:`Int <type-ghc-types-int-37261>` \-\> \[a\] \-\> \[\[a\]\]

  Splits a list into chunks of length @n@\.
  @n@ must be strictly greater than zero\.
  The last chunk will be shorter than @n@ in case the length of the input is
  not divisible by @n@\.

.. _function-da-list-delete-22340:

`delete <function-da-list-delete-22340_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> a \-\> \[a\] \-\> \[a\]

  ``delete x`` removes the first occurrence of ``x`` from its list argument\.
  For example,

  .. code-block:: daml

    > delete "a" ["b","a","n","a","n","a"]
    ["b","n","a","n","a"]


  It is a special case of 'deleteBy', which allows the programmer to
  supply their own equality test\.

.. _function-da-list-deleteby-50465:

`deleteBy <function-da-list-deleteby-50465_>`_
  \: (a \-\> a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> a \-\> \[a\] \-\> \[a\]

  The 'deleteBy' function behaves like 'delete', but takes a
  user\-supplied equality predicate\.

  .. code-block:: daml

    > deleteBy (<=) 4 [1..10]
    [1,2,3,5,6,7,8,9,10]

.. _function-da-list-x-54181:

`(\\\\\\\\) <function-da-list-x-54181_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> \[a\] \-\> \[a\] \-\> \[a\]

  The ``\\`` function is list difference (non\-associative)\.
  In the result of ``xs \\ ys``, the first occurrence of each element of
  ``ys`` in turn (if any) has been removed from ``xs``\. Thus

  .. code-block:: daml

    (xs ++ ys) \\ xs == ys


  Note this function is *O(n\*m)* given lists of size *n* and *m*\.

.. _function-da-list-singleton-17649:

`singleton <function-da-list-singleton-17649_>`_
  \: a \-\> \[a\]

  Produce a singleton list\.

  .. code-block:: daml

    >>> singleton True
    [True]

.. _function-da-list-bangbang-90127:

`(!!) <function-da-list-bangbang-90127_>`_
  \: \[a\] \-\> :ref:`Int <type-ghc-types-int-37261>` \-\> a

  List index (subscript) operator, starting from 0\.
  For example, ``xs !! 2`` returns the third element in ``xs``\.
  Raises an error if the index is not suitable for the given list\.
  The function has complexity *O(n)* where *n* is the index given,
  unlike in languages such as Java where array indexing is *O(1)*\.

.. _function-da-list-elemindex-4965:

`elemIndex <function-da-list-elemindex-4965_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> a \-\> \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Int <type-ghc-types-int-37261>`

  Find index of element in given list\.
  Will return ``None`` if not found\.

.. _function-da-list-findindex-82181:

`findIndex <function-da-list-findindex-82181_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Int <type-ghc-types-int-37261>`

  Find index, given predicate, of first matching element\.
  Will return ``None`` if not found\.
