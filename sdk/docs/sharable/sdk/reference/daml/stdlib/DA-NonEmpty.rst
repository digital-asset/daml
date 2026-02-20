.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-nonempty-15701:

DA.NonEmpty
===========

Type and functions for non\-empty lists\. This module re\-exports many functions with
the same name as prelude list functions, so it is expected to import the module qualified\.
For example, with the following import list you will have access to the ``NonEmpty`` type
and any functions on non\-empty lists will be qualified, for example as ``NE.append, NE.map, NE.foldl``\:

.. code-block:: daml

  import DA.NonEmpty (NonEmpty)
  import qualified DA.NonEmpty as NE

Functions
---------

.. _function-da-nonempty-cons-63704:

`cons <function-da-nonempty-cons-63704_>`_
  \: a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a

  Prepend an element to a non\-empty list\.

.. _function-da-nonempty-append-34337:

`append <function-da-nonempty-append-34337_>`_
  \: :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a

  Append or concatenate two non\-empty lists\.

.. _function-da-nonempty-map-69362:

`map <function-da-nonempty-map-69362_>`_
  \: (a \-\> b) \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` b

  Apply a function over each element in the non\-empty list\.

.. _function-da-nonempty-nonempty-24939:

`nonEmpty <function-da-nonempty-nonempty-24939_>`_
  \: \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` (:ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a)

  Turn a list into a non\-empty list, if possible\. Returns
  ``None`` if the input list is empty, and ``Some`` otherwise\.

.. _function-da-nonempty-singleton-99101:

`singleton <function-da-nonempty-singleton-99101_>`_
  \: a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a

  A non\-empty list with a single element\.

.. _function-da-nonempty-tolist-15474:

`toList <function-da-nonempty-tolist-15474_>`_
  \: :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> \[a\]

  Turn a non\-empty list into a list (by forgetting that it is not empty)\.

.. _function-da-nonempty-reverse-64050:

`reverse <function-da-nonempty-reverse-64050_>`_
  \: :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a

  Reverse a non\-empty list\.

.. _function-da-nonempty-find-73910:

`find <function-da-nonempty-find-73910_>`_
  \: (a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` a

  Find an element in a non\-empty list\.

.. _function-da-nonempty-deleteby-6333:

`deleteBy <function-da-nonempty-deleteby-6333_>`_
  \: (a \-\> a \-\> :ref:`Bool <type-ghc-types-bool-66265>`) \-\> a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> \[a\]

  The 'deleteBy' function behaves like 'delete', but takes a
  user\-supplied equality predicate\.

.. _function-da-nonempty-delete-59160:

`delete <function-da-nonempty-delete-59160_>`_
  \: :ref:`Eq <class-ghc-classes-eq-22713>` a \=\> a \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> \[a\]

  Remove the first occurrence of x from the non\-empty list, potentially
  removing all elements\.

.. _function-da-nonempty-foldl1-17561:

`foldl1 <function-da-nonempty-foldl1-17561_>`_
  \: (a \-\> a \-\> a) \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> a

  Apply a function repeatedly to pairs of elements from a non\-empty list,
  from the left\. For example, ``foldl1 (+) (NonEmpty 1 [2,3,4]) = ((1 + 2) + 3) + 4``\.

.. _function-da-nonempty-foldr1-43627:

`foldr1 <function-da-nonempty-foldr1-43627_>`_
  \: (a \-\> a \-\> a) \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> a

  Apply a function repeatedly to pairs of elements from a non\-empty list,
  from the right\. For example, ``foldr1 (+) (NonEmpty 1 [2,3,4]) = 1 + (2 + (3 + 4))``\.

.. _function-da-nonempty-foldr-65043:

`foldr <function-da-nonempty-foldr-65043_>`_
  \: (a \-\> b \-\> b) \-\> b \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> b

  Apply a function repeatedly to pairs of elements from a non\-empty list,
  from the right, with a given initial value\. For example,
  ``foldr (+) 0 (NonEmpty 1 [2,3,4]) = 1 + (2 + (3 + (4 + 0)))``\.

.. _function-da-nonempty-foldra-91227:

`foldrA <function-da-nonempty-foldra-91227_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> b \-\> m b) \-\> b \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> m b

  The same as ``foldr`` but running an action each time\.

.. _function-da-nonempty-foldr1a-13463:

`foldr1A <function-da-nonempty-foldr1a-13463_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> a \-\> m a) \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> m a

  The same as ``foldr1`` but running an action each time\.

.. _function-da-nonempty-foldl-91113:

`foldl <function-da-nonempty-foldl-91113_>`_
  \: (b \-\> a \-\> b) \-\> b \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> b

  Apply a function repeatedly to pairs of elements from a non\-empty list,
  from the left, with a given initial value\. For example,
  ``foldl (+) 0 (NonEmpty 1 [2,3,4]) = (((0 + 1) + 2) + 3) + 4``\.

.. _function-da-nonempty-foldla-69961:

`foldlA <function-da-nonempty-foldla-69961_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (b \-\> a \-\> m b) \-\> b \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> m b

  The same as ``foldl`` but running an action each time\.

.. _function-da-nonempty-foldl1a-63665:

`foldl1A <function-da-nonempty-foldl1a-63665_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> a \-\> m a) \-\> :ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` a \-\> m a

  The same as ``foldl1`` but running an action each time\.
