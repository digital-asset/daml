.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Expressions
######################

This page gives reference information for Daml expressions that are not :doc:`updates <updates>`.

.. _daml-ref-def:

Definitions
***********

Use assignment to bind values or functions at the top level of a Daml file or in a contract template body.

Values
======

For example:

.. literalinclude:: ../code-snippets/TubeSurfaceArea.daml
   :language: daml
   :lines: 9

The fact that ``pi`` has type ``Decimal`` is inferred from the value. To explicitly annotate the type, mention it after a colon following the variable name:

.. literalinclude:: ../code-snippets/TubeSurfaceArea2.daml
   :language: daml
   :lines: 11

Functions
=========

You can define functions. Here's an example: a function for computing the surface area of a tube:

.. literalinclude:: ../code-snippets/TubeSurfaceArea.daml
   :language: daml
   :lines: 14-16

Here you see:

- the name of the function
- the function's type signature ``Decimal -> Decimal -> Decimal``

  This means it takes two Decimals and returns another Decimal.
- the definition ``= 2.0 * pi * r * h`` (which uses the previously defined ``pi``)

Arithmetic Operators
********************

.. list-table::
   :widths: 15 30
   :header-rows: 1

   * - Operator
     - Works for
   * - ``+``
     - ``Int``, ``Decimal``, ``RelTime``
   * - ``-``
     - ``Int``, ``Decimal``, ``RelTime``
   * - ``*``
     - ``Int``, ``Decimal``
   * - ``/`` (integer division)
     - ``Int``
   * - ``%`` (integer remainder operation)
     - ``Int``
   * - ``^`` (integer exponentiation)
     - ``Int``

The result of the modulo operation has the same sign as the dividend:

* ``7 / 3`` and ``(-7) / (-3)`` evaluate to ``2``
* ``(-7) / 3`` and ``7 / (-3)`` evaluate to ``-2``
* ``7 % 3`` and ``7 % (-3)`` evaluate to ``1``
* ``(-7) % 3`` and ``(-7) % (-3)`` evaluate to ``-1``

To write infix expressions in prefix form, wrap the operators in parentheses. For example, ``(+) 1 2`` is another way of writing ``1 + 2``.

Comparison Operators
********************

.. list-table::
   :widths: 15 30
   :header-rows: 1

   * - Operator
     - Works for
   * - ``<``, ``<=``, ``>``, ``>=``
     - ``Bool``, ``Text``, ``Int``, ``Decimal``, ``Party``, ``Time``
   * - ``==``, ``/=``
     - ``Bool``, ``Text``, ``Int``, ``Decimal``, ``Party``, ``Time``, and :ref:`identifiers of contracts <daml-ref_contract-identifiers>` stemming from the same contract template

Logical Operators
*****************

The logical operators in Daml are:

- ``not`` for negation, e.g., ``not True == False``
- ``&&`` for conjunction, where ``a && b == and a b``
- ``||`` for disjunction, where ``a || b == or a b``

for ``Bool`` variables ``a`` and ``b``.

.. _daml-ref-if-then-else:

If-then-else
************

You can use conditional *if-then-else* expressions, for example:

.. code-block:: daml

  if owner == scroogeMcDuck then "sell" else "buy"

.. _daml-ref-let:

Let
***

To bind values or functions to be in scope beneath the expression, use the block keyword ``let``:

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :start-after: -- BEGIN_LET_EXPRESSION
   :end-before: -- END_LET_EXPRESSION

You can use ``let`` inside ``do`` blocks:

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :start-after: -- BEGIN_LET_IN_DO
   :end-before: -- END_LET_IN_DO

Lastly, a ``template`` may contain a single ``let`` block.

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :start-after: -- BEGIN_LET_IN_TEMPLATE
   :end-before: -- END_LET_IN_TEMPLATE

.. TODO: check you can still have a let block inside a template?
