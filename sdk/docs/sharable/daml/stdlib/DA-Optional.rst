.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-optional-38505:

DA.Optional
===========

The ``Optional`` type encapsulates an optional value\. A value of type
``Optional a`` either contains a value of type ``a`` (represented as ``Some a``),
or it is empty (represented as ``None``)\. Using ``Optional`` is a good way
to deal with errors or exceptional cases without resorting to
drastic measures such as error\.

The Optional type is also an action\. It is a simple kind of error
action, where all errors are represented by ``None``\. A richer
error action can be built using the ``Either`` type\.

Functions
---------

.. _function-da-optional-fromsome-59859:

`fromSome <function-da-optional-fromsome-59859_>`_
  \: :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> a

  The ``fromSome`` function extracts the element out of a ``Some`` and
  throws an error if its argument is ``None``\.

  Note that in most cases you should prefer using ``fromSomeNote``
  to get a better error on failures\.

.. _function-da-optional-fromsomenote-25463:

`fromSomeNote <function-da-optional-fromsomenote-25463_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> a

  Like ``fromSome`` but with a custom error message\.

.. _function-da-optional-catoptionals-11568:

`catOptionals <function-da-optional-catoptionals-11568_>`_
  \: \[:ref:`Optional <type-da-internal-prelude-optional-37153>` a\] \-\> \[a\]

  The ``catOptionals`` function takes a list of ``Optionals`` and returns a
  list of all the ``Some`` values\.

.. _function-da-optional-listtooptional-83598:

`listToOptional <function-da-optional-listtooptional-83598_>`_
  \: \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` a

  The ``listToOptional`` function returns ``None`` on an empty list or
  ``Some`` a where a is the first element of the list\.

.. _function-da-optional-optionaltolist-83426:

`optionalToList <function-da-optional-optionaltolist-83426_>`_
  \: :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> \[a\]

  The ``optionalToList`` function returns an empty list when given
  ``None`` or a singleton list when not given ``None``\.

.. _function-da-optional-fromoptional-77879:

`fromOptional <function-da-optional-fromoptional-77879_>`_
  \: a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> a

  The ``fromOptional`` function takes a default value and a ``Optional``
  value\. If the ``Optional`` is ``None``, it returns the default values
  otherwise, it returns the value contained in the ``Optional``\.

.. _function-da-optional-issome-25261:

`isSome <function-da-optional-issome-25261_>`_
  \: :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isSome`` function returns ``True`` iff its argument is of the
  form ``Some _``\.

.. _function-da-optional-isnone-84783:

`isNone <function-da-optional-isnone-84783_>`_
  \: :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  The ``isNone`` function returns ``True`` iff its argument is
  ``None``\.

.. _function-da-optional-mapoptional-4330:

`mapOptional <function-da-optional-mapoptional-4330_>`_
  \: (a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b) \-\> \[a\] \-\> \[b\]

  The ``mapOptional`` function is a version of ``map`` which can throw out
  elements\. In particular, the functional argument returns something
  of type ``Optional b``\. If this is ``None``, no element is added on to
  the result list\. If it is ``Some b``, then ``b`` is included in the
  result list\.

.. _function-da-optional-whensome-82167:

`whenSome <function-da-optional-whensome-82167_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` m \=\> :ref:`Optional <type-da-internal-prelude-optional-37153>` a \-\> (a \-\> m ()) \-\> m ()

  Perform some operation on ``Some``, given the field inside the
  ``Some``\.

.. _function-da-optional-findoptional-83634:

`findOptional <function-da-optional-findoptional-83634_>`_
  \: (a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b) \-\> \[a\] \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b

  The ``findOptional`` returns the value of the predicate at the first
  element where it returns ``Some``\. ``findOptional`` is similar to ``find`` but it
  allows you to return a value from the predicate\. This is useful both as a more
  type safe version if the predicate corresponds to a pattern match
  and for performance to avoid duplicating work performed in the predicate\.
