.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-action-7169:

DA.Action
=========

Action

Functions
---------

.. _function-da-action-when-35467:

`when <function-da-action-when-35467_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> :ref:`Bool <type-ghc-types-bool-66265>` \-\> f () \-\> f ()

  Conditional execution of ``Action`` expressions\. For example,

  .. code-block:: daml

      when final (archive contractId)


  will archive the contract ``contractId`` if the Boolean value ``final`` is
  ``True``, and otherwise do nothing\.

  This function has short\-circuiting semantics, i\.e\., when both arguments are
  present and the first arguments evaluates to ``False``, the second argument
  is not evaluated at all\.

.. _function-da-action-unless-8539:

`unless <function-da-action-unless-8539_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> :ref:`Bool <type-ghc-types-bool-66265>` \-\> f () \-\> f ()

  The reverse of ``when``\.

  This function has short\-circuiting semantics, i\.e\., when both arguments are
  present and the first arguments evaluates to ``True``, the second argument
  is not evaluated at all\.

.. _function-da-action-foldra-2803:

`foldrA <function-da-action-foldra-2803_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> b \-\> m b) \-\> b \-\> \[a\] \-\> m b

  The ``foldrA`` is analogous to ``foldr``, except that its result is
  encapsulated in an action\. Note that ``foldrA`` works from right\-to\-left
  over the list arguments\.

.. _function-da-action-foldr1a-55935:

`foldr1A <function-da-action-foldr1a-55935_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> a \-\> m a) \-\> \[a\] \-\> m a

  ``foldr1A`` is like ``foldrA`` but raises an error when presented
  with an empty list argument\.

.. _function-da-action-foldla-78897:

`foldlA <function-da-action-foldla-78897_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (b \-\> a \-\> m b) \-\> b \-\> \[a\] \-\> m b

  ``foldlA`` is analogous to ``foldl``, except that its result is
  encapsulated in an action\. Note that ``foldlA`` works from
  left\-to\-right over the list arguments\.

.. _function-da-action-foldl1a-65193:

`foldl1A <function-da-action-foldl1a-65193_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> a \-\> m a) \-\> \[a\] \-\> m a

  The ``foldl1A`` is like ``foldlA`` but raises an errors when
  presented with an empty list argument\.

.. _function-da-action-filtera-13011:

`filterA <function-da-action-filtera-13011_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` m \=\> (a \-\> m :ref:`Bool <type-ghc-types-bool-66265>`) \-\> \[a\] \-\> m \[a\]

  Filters the list using the applicative function\: keeps only the elements where the predicate holds\.
  Example\: given a collection of Iou contract IDs one can find only the GBPs\.

  .. code-block:: daml

    filterA (fmap (\iou -> iou.currency == "GBP") . fetch) iouCids

.. _function-da-action-replicatea-98867:

`replicateA <function-da-action-replicatea-98867_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` m \=\> :ref:`Int <type-ghc-types-int-37261>` \-\> m a \-\> m \[a\]

  ``replicateA n act`` performs the action ``n`` times, gathering the
  results\.

.. _function-da-action-replicatea-83733:

`replicateA_ <function-da-action-replicatea-83733_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` m \=\> :ref:`Int <type-ghc-types-int-37261>` \-\> m a \-\> m ()

  Like ``replicateA``, but discards the result\.

.. _function-da-action-gteqgt-60955:

`(>=>) <function-da-action-gteqgt-60955_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (a \-\> m b) \-\> (b \-\> m c) \-\> a \-\> m c

  Left\-to\-right composition of Kleisli arrows\.

.. _function-da-action-lteqlt-31871:

`(<=<) <function-da-action-lteqlt-31871_>`_
  \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (b \-\> m c) \-\> (a \-\> m b) \-\> a \-\> m c

  Right\-to\-left composition of Kleisli arrows\. @('\>\=\>')@, with the arguments
  flipped\.
