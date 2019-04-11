.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: data types
#####################

This page gives reference information on DAML's data types:

.. contents:: :local:

.. _daml-ref-built-in-types:

Built-in types
**************

Table of built-in primitive types
=================================

.. list-table::
   :widths: 10 15 10 30
   :header-rows: 1

   * - Type
     - For
     - Example
     - Notes
   * - ``Int``
     - integers
     - ``1``,  ``1000000``, ``1_000_000``
     - ``Int`` values are signed 64-bit integers which represent numbers between ``-9,223,372,036,854,775,808`` and ``9,223,372,036,854,775,807`` inclusive. Arithmetic operations raise an error on overflows and division by ``0``. To make long numbers more readable you can optionally add underscores.
   * - ``Decimal``
     - fixed point decimals
     - ``1.0``
     - ``Decimal`` values are rational numbers with precision 38 and scale 10: numbers of the form ``x / 10^10`` where x is an integer with ``|x| < 10^38``.
   * - ``Text``
     - strings
     - ``"hello"``
     - ``Text`` values are strings of characters enclosed by double quotes.
   * - ``Bool``
     - boolean values
     - ``True``, ``False``
     -
   * - ``Party``
     - unicode string representing a party
     - ``alice <- getParty "Alice"``
     - Every *party* in a DAML system has a unique identifier of type ``Party``. To create a value of type ``Party``, use binding on the result of calling ``getParty``. The party text can only contain alphanumeric characters, ``-``, ``_`` and spaces.
   * - ``Date``
     - models dates
     - ``date 2007 Apr 5``
     - To create a value of type ``Date``, use the function ``date`` (to get this function, import ``DA.Date``).
   * - ``Time``
     - models absolute time (UTC)
     - ``time (date 2007 Apr 5) 14 30 05``
     - ``Time`` values have microsecond precision. To create a value of type ``Time``, use a ``Date`` and the function ``time`` (to get this function, import ``DA.Time``). 
   * - ``RelTime``
     - models differences between time values
     - ``seconds 1``, ``seconds (-2)``
     - ``seconds 1`` and ``seconds (-2)`` represent the values for 1 and -2 seconds. There are no literals for ``RelTime``. Instead they are created using one of ``days``, ``hours``, ``minutes`` and ``seconds`` (to get these functions, import ``DA.Time``).

Escaping characters
===================

``Text`` literals support backslash escapes to include their delimiter (``\"``) and a backslash itself (``\\``).

Time
====

Definition of time on the ledger is a property of the execution environment. DAML assumes there is a shared understanding of what time is among the stakeholders of contracts.

Lists
*****

``[a]`` is the built-in data type for a list of elements of type ``a``. The empty list is denoted by ``[]`` and ``[1, 3, 2]`` is an example of a list of type ``[Int]``.

You can also construct lists using ``[]`` (the empty
list) and ``::`` (which is an operator that appends an element to the front of a list). For example:

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :lines: 51-53

Summing a list
==============

To sum a list, use a *fold* (because there are no loops in DAML). See :ref:`daml-ref-folding` for details.

.. TODO - reference std lib documentation

.. _daml-ref-record-types:

Records and record types
************************

You declare a new record type using the ``data`` and ``with`` keyword:

.. ExcludeFromDamlParsing
.. code-block:: daml

  data MyRecord = MyRecord
    with
      label1 : type1
      label2 : type2
      ...
      labelN : typeN
    deriving (Eq, Show)

where:

- ``label1``, ``label2``, ..., ``labelN`` are *labels*, which must be unique in the record type
- ``type1``, ``type2``, ..., ``typeN`` are the types of the fields

There's an alternative way to write record types:

.. ExcludeFromDamlParsing
.. code-block:: daml

  data MyRecord = MyRecord { label1 : type1; label2 : type2; ...; labelN : typeN }
    deriving (Eq, Show)

The format using ``with`` and the format using ``{ }`` are exactly the same syntactically. The main difference is that when you use ``with``, you can use newlines and proper indentation to avoid the delimiting semicolons.

The ``deriving (Eq, Show)`` ensures the data type can be compared (using ``==``) and displayed (using ``show``). The line starting ``deriving`` is required for data types used in fields of a ``template``.

In general, add the ``deriving`` unless the data type contains function types (e.g. ``Int -> Int``), which cannot be compared or shown.

For example:

.. ExcludeFromDamlParsing
.. code-block:: daml

  -- This is a record type with two fields, called first and second,
  -- both of type `Int`
  data MyRecord = MyRecord with first : Int; second : Int
    deriving (Eq, Show)

  -- An example value of this type is:
  newRecord = MyRecord with first = 1; second = 2

  -- You can also write:
  newRecord = MyRecord 1 2

.. _daml-ref-data-constructors:

Data constructors
=================

You can use ``data`` keyword to define a new data type, for example ``data Floor a = Floor a`` for some type ``a``.

The first ``Floor`` in the expression is the *type constructor*. The second ``Floor`` is a *data constructor* that can be used to specify values of the ``Floor Int``
type: for example, ``Floor 0``, ``Floor 1``.

In DAML, data constructors may take *at most one argument*.

An example of a data constructor with zero arguments is ``data Empty = Empty {}``. The only value of the ``Empty`` type is ``Empty``.

.. note::
  In ``data Confusing = Int``, the ``Int`` is a data constructor with no arguments. It has nothing to do with the built-in ``Int`` type.


Accessing record fields
=======================

To access the fields of a record type, use dot notation. For example:

.. ExcludeFromDamlParsing
.. code-block:: daml

  -- Access the value of the field `first`
  val.first

  -- Access the value of the field `second`
  val.second

Updating record fields
======================

You can also use the ``with`` keyword to create a new record on the basis of an existing replacing select fields.

For example:

.. ExcludeFromDamlParsing
.. code-block:: daml

  myRecord = MyRecord with first = 1; second = 2

  myRecord2 = myRecord with second = 5

produces the new record value ``MyRecord with first = 1; second = 5``.

If you have a variable with the same name as the label, DAML lets you use this without assigning it to make things look nicer:

.. ExcludeFromDamlParsing
.. code-block:: daml

  -- if you have a variable called `second` equal to 5
  second = 5

  -- you could construct the same value as before with
  myRecord2 = myRecord with second = second

  -- or with
  myRecord3 = MyRecord with first = 1; second = second

  -- but DAML has a nicer way of putting this:
  myRecord4 = MyRecord with first = 1; second

  -- or even
  myRecord5 = r with second

.. note:: The ``with`` keyword binds more strongly than function application. So for a function, say ``return``, either write ``return IntegerCoordinate with first = 1; second = 5`` or ``return (IntegerCoordinate {first = 1; second = 5})``, where the latter expression is enclosed in parentheses.

.. _daml-ref-parameterized-types:

Parameterized data types
========================

DAML supports parameterized data types.

For example, to express a more general type for 2D coordinates:

.. ExcludeFromDamlParsing
.. code-block:: daml

  -- Here, a and b are type parameters.
  -- The Coordinate after the data keyword is a type constructor.
  data Coordinate a b = Coordinate with first : a; second : b

An example of a type that can be constructed with ``Coordinate`` is ``Coordinate Int Int``.

.. _daml-ref-type-synonyms:

Type synonyms
*************

To declare a synonym for a type, use the ``type`` keyword.

For example:

.. ExcludeFromDamlParsing
.. code-block:: daml

  type IntegerTuple = (Int, Int)

This makes ``IntegerTuple`` and ``(Int, Int)``  synonyms: they have the same type and can be used interchangeably.

You can use the ``type`` keyword for any type, including :ref:`daml-ref-built-in-types`.

Function types
==============

A function's type includes its parameter and result types. A function ``foo`` with two parameters has type ``ParamType1 -> ParamType2 -> ReturnType``.

Note that this can be treated as any other type. You could for instance give it a synonym using ``type FooType = ParamType1 -> ParamType2 -> ReturnType``.

Algebraic data types
********************

An algebraic data type is a composite type: a type formed by a combination of other types. The enumeration data type is an example. This section introduces more powerful algebraic data types.

Product types
=============

The following data constructor is not valid in DAML: ``data AlternativeCoordinate a b = AlternativeCoordinate a b``. This is because data constructors can only have one argument.

To get around this, wrap the values in a :ref:`record <daml-ref-record-types>`:
``data Coordinate a b = Coordinate {first: a; second: b}``.

These kinds of types are called *product* types.

A way of thinking about this is that the ``Coordinate Int Int`` type has a first and second dimension (that is, a 2D product space). By adding an extra type to the record, you get a third dimension, and so on.

.. _daml-ref-sum-types:

Sum types
=========

Sum types capture the notion of being of one kind or another.

An example is the built-in data type ``Bool``. This is defined by ``data Bool = True | False``, where ``True`` and ``False`` are data constructors with zero arguments . This means that a ``Bool`` value is either ``True`` or ``False`` and cannot be instantiated with any other value.

A very useful sum type is ``data Optional a = None | Some a``. It is part of
the :doc:`DAML standard library </daml/stdlib/index>`.

``Optional`` captures the concept of a box, which can be empty or contain a value of type ``a``.

``Optional`` is a sum type constructor taking a type ``a`` as parameter. It produces the sum type defined by the data constructors ``None`` and ``Some``.

The ``Some`` data constructor takes one argument, and it expects a value of type ``a`` as a parameter.

Pattern matching
================

You can match a value to a specific pattern using the ``case`` keyword.

The pattern is expressed with data constructors. For example, the ``Optional Int`` sum type:

.. literalinclude:: ../code-snippets/PatternMatching.daml
   :language: daml
   :lines: 8-17

In the ``optionalIntegerToText`` function, the ``case`` construct first tries to
match the ``x`` argument against the ``None`` data constructor, and in case of a match, the ``"Box is empty"`` text is returned. In case of no match, a match is attempted for ``x`` against the next pattern in the list, i.e., with the ``Some`` data constructor. In case of a match, the content of the value attached to the ``Some`` label is bound to the ``val`` variable, which is then used in the corresponding output text string.

Note that all patterns in the case construct need to be *complete*, i.e., for each ``x`` there must be at least one pattern that matches. The patterns are tested from top to bottom, and the expression for the first pattern that matches will be executed. Note that ``_`` can be used as a catch-all pattern.

You could also case distinguish a ``Bool`` variable using the ``True`` and ``False`` data constructors and achieve the same behavior as an if-then-else expression.

As an example, the following is an expression for a ``Text``:

.. literalinclude:: ../code-snippets/PatternMatching.daml
   :language: daml
   :lines: 21-26

Notice the use of nested pattern matching above.

.. note:: An underscore was used in place of a variable name. The reason for this is that :doc:`DAML Studio <../daml-studio>` produces a warning for all variables that are not being used. This is useful in detecting unused variables. You can suppress the warning by naming the variable with an initial underscore.
