.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-math-30023:

DA.Math
=======

Math \- Utility Math functions for ``Decimal``
The this library is designed to give good precision, typically giving 9 correct decimal places\.
The numerical algorithms run with many iterations to achieve that precision and are interpreted
by the Daml runtime so they are not performant\. Their use is not advised in performance critical
contexts\.

Functions
---------

.. _function-da-math-starstar-89123:

`(**) <function-da-math-starstar-89123_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  Take a power of a number Example\: ``2.0 ** 3.0 == 8.0``\.

.. _function-da-math-sqrt-24467:

`sqrt <function-da-math-sqrt-24467_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  Calculate the square root of a Decimal\.

  .. code-block:: daml

    >>> sqrt 1.44
    1.2

.. _function-da-math-exp-84235:

`exp <function-da-math-exp-84235_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  The exponential function\. Example\: ``exp 0.0 == 1.0``

.. _function-da-math-log-52192:

`log <function-da-math-log-52192_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  The natural logarithm\. Example\: ``log 10.0 == 2.30258509299``

.. _function-da-math-logbase-64267:

`logBase <function-da-math-logbase-64267_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  The logarithm of a number to a given base\. Example\: ``log 10.0 100.0 == 2.0``

.. _function-da-math-sin-61636:

`sin <function-da-math-sin-61636_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  ``sin`` is the sine function

.. _function-da-math-cos-82859:

`cos <function-da-math-cos-82859_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  ``cos`` is the cosine function

.. _function-da-math-tan-54959:

`tan <function-da-math-tan-54959_>`_
  \: :ref:`Decimal <type-ghc-types-decimal-18135>` \-\> :ref:`Decimal <type-ghc-types-decimal-18135>`

  ``tan`` is the tangent function
