.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-either-91022:

DA.Either
=========

The ``Either`` type represents values with two possibilities\.

It is sometimes used to represent a value which is either correct
or an error\. By convention, the ``Left`` constructor is used to hold
an error value and the ``Right`` constructor is used to hold a correct
value (mnemonic\: \"right\" also means correct)\.

Functions
---------

.. _function-da-either-lefts-59601:

`lefts <function-da-either-lefts-59601_>`_
  \: \[:ref:`Either <type-da-types-either-56020>` a b\] \-\> \[a\]

  Extracts all the ``Left`` elements from a list\.

.. _function-da-either-rights-20455:

`rights <function-da-either-rights-20455_>`_
  \: \[:ref:`Either <type-da-types-either-56020>` a b\] \-\> \[b\]

  Extracts all the ``Right`` elements from a list\.

.. _function-da-either-partitioneithers-19904:

`partitionEithers <function-da-either-partitioneithers-19904_>`_
  \: \[:ref:`Either <type-da-types-either-56020>` a b\] \-\> (\[a\], \[b\])

  Partitions a list of ``Either`` into two lists, the ``Left`` and
  ``Right`` elements respectively\. Order is maintained\.

.. _function-da-either-isleft-96021:

`isLeft <function-da-either-isleft-96021_>`_
  \: :ref:`Either <type-da-types-either-56020>` a b \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Return ``True`` if the given value is a ``Left``\-value, ``False``
  otherwise\.

.. _function-da-either-isright-36975:

`isRight <function-da-either-isright-36975_>`_
  \: :ref:`Either <type-da-types-either-56020>` a b \-\> :ref:`Bool <type-ghc-types-bool-66265>`

  Return ``True`` if the given value is a ``Right``\-value, ``False``
  otherwise\.

.. _function-da-either-fromleft-63875:

`fromLeft <function-da-either-fromleft-63875_>`_
  \: a \-\> :ref:`Either <type-da-types-either-56020>` a b \-\> a

  Return the contents of a ``Left``\-value, or a default value
  in case of a ``Right``\-value\.

.. _function-da-either-fromright-27657:

`fromRight <function-da-either-fromright-27657_>`_
  \: b \-\> :ref:`Either <type-da-types-either-56020>` a b \-\> b

  Return the contents of a ``Right``\-value, or a default value
  in case of a ``Left``\-value\.

.. _function-da-either-optionaltoeither-21876:

`optionalToEither <function-da-either-optionaltoeither-21876_>`_
  \: a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b \-\> :ref:`Either <type-da-types-either-56020>` a b

  Convert a ``Optional`` value to an ``Either`` value, using the supplied
  parameter as the ``Left`` value if the ``Optional`` is ``None``\.

.. _function-da-either-eithertooptional-89140:

`eitherToOptional <function-da-either-eithertooptional-89140_>`_
  \: :ref:`Either <type-da-types-either-56020>` a b \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b

  Convert an ``Either`` value to a ``Optional``, dropping any value in
  ``Left``\.

.. _function-da-either-maybetoeither-6635:

`maybeToEither <function-da-either-maybetoeither-6635_>`_
  \: a \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b \-\> :ref:`Either <type-da-types-either-56020>` a b

.. _function-da-either-eithertomaybe-94811:

`eitherToMaybe <function-da-either-eithertomaybe-94811_>`_
  \: :ref:`Either <type-da-types-either-56020>` a b \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` b
