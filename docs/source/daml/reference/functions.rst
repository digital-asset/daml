.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: Functions
####################

This page gives reference information on functions in Daml.

Daml is a functional language. It lets you apply functions partially and also have functions that take other functions as arguments. This page discusses these *higher-order functions*.

Defining Functions
******************

In :doc:`expressions`, the ``tubeSurfaceArea`` function was defined as:

.. literalinclude:: ../code-snippets/TubeSurfaceArea.daml
   :language: daml
   :lines: 14-16

You can define this function equivalently using lambdas, involving ``\``, a sequence of parameters, and an arrow ``->`` as:

.. literalinclude:: ../code-snippets/TubeSurfaceArea2.daml
   :language: daml
   :lines: 13-14

Partial Application
*******************

The type of the ``tubeSurfaceArea`` function described previously, is ``Decimal -> Decimal -> Decimal``. An equivalent, but more instructive, way to read its type is: ``Decimal -> (Decimal -> Decimal)``: saying that ``tubeSurfaceArea`` is a function that takes *one* argument and returns another function.

So ``tubeSurfaceArea`` expects one argument of type ``Decimal`` and returns a function of type ``Decimal -> Decimal``. In other words, this function returns another function. *Only the last application of an argument yields a non-function.*

This is called *currying*: currying is the process of converting a function of multiple arguments to a function that takes just a single argument and returns another function. In Daml, all functions are curried.

This doesn't affect things that much. If you use functions in the classical way (by applying them to all parameters) then there is no difference.

If you only apply a few arguments to the function, this is called *partial application*. The result is a function with partially defined arguments. For example:

.. literalinclude:: ../code-snippets/Functions.daml
   :language: daml
   :start-after: PARTIAL_APPLICATION_START
   :end-before: PARTIAL_APPLICATION_END

You could also define equivalent lambda functions:

.. literalinclude:: ../code-snippets/Functions.daml
   :language: daml
   :start-after: PARTIAL_LAMBDA_START
   :end-before: PARTIAL_LAMBDA_END

Functions are Values
********************

The function type can be explicitly added to the ``tubeSurfaceArea`` function (when it is written with the lambda notation):

.. literalinclude:: ../code-snippets/TubeSurfaceArea2.daml
   :language: daml
   :lines: 8-14

Note that ``tubeSurfaceArea : BinaryDecimalFunction = ...`` follows the same pattern as when binding values, e.g., ``pi : Decimal = 3.14159265359``.

Functions have types, just like values. Which means they can be used just like normal variables. In fact, in Daml, functions are values.

This means a function can take another function as an argument. For example, define a function ``applyFilter: (Int -> Int -> Bool) -> Int -> Int -> Bool`` which applies the first argument, a higher-order function, to the second and the third arguments to yield the result.

.. literalinclude:: ../code-snippets/Functions.daml
   :language: daml
   :start-after: HIGHER_ORDER_START
   :end-before: HIGHER_ORDER_END

The :ref:`daml-ref-folding` section looks into two useful built-in functions, ``foldl`` and ``foldr``, that also take a function as an argument.

.. note:: Daml does not allow functions as parameters of contract templates and contract choices. However, a follow up of a choice can use built-in functions, defined at the top level or in the contract template body.

Generic Functions
*****************

A function is *parametrically polymorphic* if it behaves uniformly for all types, in at least one of its type parameters. For example, you can define function composition as follows:

.. literalinclude:: ../code-snippets/Functions.daml
   :language: daml
   :start-after: GENERIC_FUNCTION_START
   :end-before: GENERIC_FUNCTION_END

where ``a``, ``b``, and ``c`` are any data types. Both ``compose ((+) 4) ((*) 2) 3 == 10`` and ``compose not ((&&) True) False`` evaluate to ``True``. Note that ``((+) 4)`` has type ``Int -> Int``, whereas ``not`` has type ``Bool -> Bool``.

You can find many other generic functions including this one in the :doc:`Daml standard library </daml/stdlib/index>`.

.. note:: Daml currently does not support generic functions for a specific set of types, such as ``Int`` and ``Decimal`` numbers. For example, ``sum (x: a) (y: a) = x + y`` is undefined when ``a`` equals the type ``Party``. *Bounded polymorphism* might be added to Daml in a later version.
