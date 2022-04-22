.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Built-in Functions
#############################

This page gives reference information on built-in functions for working with a variety of common concepts.

Work with Time
**************

Daml has these built-in functions for working with time:

- ``datetime``: creates a ``Time`` given year, month, day, hours, minutes, and seconds as argument.
- ``subTime``: subtracts one time from another. Returns the ``RelTime`` difference between ``time1`` and ``time2``.
- ``addRelTime``: add times. Takes a ``Time`` and ``RelTime`` and adds the ``RelTime`` to the ``Time``.
- ``days``, ``hours``, ``minutes``, ``seconds``: constructs a ``RelTime`` of the specified length.
- ``pass``: (in :ref:`Daml Script tests <testing-using-script>` only) use ``pass : RelTime -> Script Time`` to advance the ledger time by the argument amount. Returns the new time.

Work with Numbers
*****************

Daml has these built-in functions for working with numbers:

- ``round``: rounds a ``Decimal`` number to ``Int``.

  ``round d`` is the *nearest* ``Int`` to ``d``. Tie-breaks are resolved by rounding away from zero, for example:

  .. code-block:: none

    round 2.5 == 3    round (-2.5) == -3
    round 3.4 == 3    round (-3.7) == -4

- ``truncate``: converts a ``Decimal`` number to ``Int``, truncating the value towards zero, for example:

  .. code-block:: none

    truncate 2.2 == 2    truncate (-2.2) == -2
    truncate 4.9 == 4    v (-4.9) == -4

- ``intToDecimal``: converts an ``Int`` to ``Decimal``.

The set of numbers expressed by ``Decimal`` is not closed under division as the result may require more than 10 decimal places to represent. For example, ``1.0 / 3.0 == 0.3333...`` is a rational number, but not a ``Decimal``.


Work with Text
**************

Daml has these built-in functions for working with text:

- ``<>`` operator: concatenates two ``Text`` values.
- ``show`` converts a value of the primitive types (``Bool``, ``Int``, ``Decimal``, ``Party``, ``Time``, ``RelTime``) to a ``Text``.

To escape text in Daml strings, use ``\``:

+-------------------+-----------------------------+
|  Character        | How to escape it            |
+===================+=============================+
| ``\``             | ``\\``                      |
+-------------------+-----------------------------+
| ``"``             | ``\"``                      |
+-------------------+-----------------------------+
| ``'``             | ``\'``                      |
+-------------------+-----------------------------+
| Newline           | ``\n``                      |
+-------------------+-----------------------------+
| Tab               | ``\t``                      |
+-------------------+-----------------------------+
| Carriage return   | ``\r``                      |
+-------------------+-----------------------------+
| Unicode           | - Decimal code: ``\33``     |
| (using ``!`` as   | - Octal code: ``\o41``      |
| an example)       | - Hexadecimal code: ``\x21``|
+-------------------+-----------------------------+

Work with Lists
***************

Daml has these built-in functions for working with lists:

- ``foldl`` and ``foldr``: see :ref:`daml-ref-folding` below.

.. _daml-ref-folding:

Fold
====

A *fold* takes:

- a binary operator
- a first *accumulator* value
- a list of values

The elements of the list are processed one-by-one (from the left in a ``foldl``, or from the right in a ``foldr``).

.. note::

  We'd usually recommend using ``foldl``, as ``foldr`` is usually slower. This is because it needs to traverse the whole list before starting to discharge its elements.

Processing goes like this:

#. The binary operator is applied to the first accumulator value and the first element in the list. This produces a second accumulator value.
#. The binary operator is applied to the *second* accumulator value and the second element in the list. This produces a third accumulator value.
#. This continues until there are no more elements in the list. Then, the last accumulator value is returned.

As an example, to sum up a list of integers in Daml:

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :start-after: -- BEGIN_SUM_LIST_EXAMPLE
   :end-before: -- END_SUM_LIST_EXAMPLE

.. To be more precise, the type of ``foldl`` is ``(acc -> a - > acc) -> acc -> List a -> acc``. ``acc`` and ``a`` are any types, and the value of ``foldl op acc1 [e1, e2, ..., eN]`` is defined by ``acc2 = op acc1 e1``, ``acc3 = op acc2 e2``, ``...``, ``accN+1 = op accN eN``, where ``accN+1`` is the resulting value.

.. Similarly, the type of ``foldr`` is ``(a -> acc - > acc) -> acc -> List a -> acc``. he value of ``foldr op acc1 [eN, ..., e2, e1]`` is defined by ``acc2 = op e1 acc1``, ``acc3 = op e2 acc2``, ``...``, ``accN+1 = op eN accN``, where ``accN+1`` is the resulting value.
