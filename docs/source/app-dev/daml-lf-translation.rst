.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

How DAML types are translated to DAML-LF
########################################

This page shows how types in DAML are translated into DAML-LF. It should help you understand and predict the generated client interfaces, which is useful when you're building a DAML-based application that uses the Ledger API or client bindings in other languages.

For an introduction to DAML-LF, see :ref:`daml-lf-intro`.

Primitive types
***************

:ref:`Built-in data types <daml-ref-built-in-types>` in DAML have straightforward mappings to DAML-LF.

This section only covers the serializable types, as these are what client applications can interact with via the generated DAML-LF. (Serializable types are ones whose values can be written in a text or binary format. So not function types, ``Update`` and ``Scenario`` types, as well as any types built up from those.)

Most built-in types have the same name in DAML-LF as in DAML. These are the exact mappings:

.. list-table::
   :widths: 10 15
   :header-rows: 1

   * - DAML primitive type
     - DAML-LF primitive type
   * - ``Int``
     - ``Int64``
   * - ``Time``
     - ``Timestamp``
   * - ``()``
     - ``Unit``
   * - ``[]``
     - ``List``
   * - ``Decimal``
     - ``Decimal``
   * - ``Text``
     - ``Text``
   * - ``Date``
     - ``Date``
   * - ``Party``
     - ``Party``
   * - ``Optional``
     - ``Optional``
   * - ``ContractId``
     - ``ContractId``

Be aware that only the DAML primitive types exported by the :ref:`Prelude <module-prelude-6842>` module map to the DAML-LF primitive types above. That means that, if you define your own type named ``Party``, it will not translate to the DAML-LF primitive ``Party``.

Tuple types
***********

DAML tuple type constructors take types ``T1, T2, …, TN`` to the type ``(T1, T2, …, TN)``. These are exposed in the DAML surface language through the :ref:`Prelude <module-prelude-6842>` module.

The equivalent DAML-LF type constructors are ``daml-prim:DA.Types:TupleN``, for each particular N (where 2 <= N <= 20). This qualified name refers to the package name (``ghc-prim``) and the module name (``GHC.Tuple``).

For example: the DAML pair type ``(Int, Text)`` is translated to ``daml-prim:DA.Types:Tuple2 Int64 Text``.

Data types
**********

DAML-LF has three kinds of data declarations:

- **Record** types, which define a collection of data
- **Variant** or **sum** types, which define a number of alternatives
- **Enum**, which defines simplified **sum** types without type parameters nor argument.

:ref:`Data type declarations in DAML <daml-ref-data-constructors>` (starting with the ``data`` keyword) are translated to record, variant or enum types. It’s sometimes not obvious what they will be translated to, so this section lists many examples of data types in DAML and their translations in DAML-LF.

.. In the tables below, the left column uses DAML 1.2 syntax and the right column uses the notation from the `DAML-LF specification <https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst>`_.

Record declarations
===================

This section uses the syntax for DAML :ref:`records <daml-ref-record-types>` with curly braces.

.. list-table::
   :widths: 10 15
   :header-rows: 1

   * - DAML declaration
     - DAML-LF translation
   * - ``data Foo = Foo { foo1: Int; foo2: Text }``
     - ``record Foo ↦ { foo1: Int64; foo2: Text }``
   * - ``data Foo = Bar { bar1: Int; bar2: Text }``
     - ``record Foo ↦ { bar1: Int64; bar2: Text }``
   * - ``data Foo = Foo { foo: Int }``
     - ``record Foo ↦ { foo: Int64 }``
   * - ``data Foo = Bar { foo: Int }``
     - ``record Foo ↦ { foo: Int64 }``
   * - ``data Foo = Foo {}``
     - ``record Foo ↦ {}``
   * - ``data Foo = Bar {}``
     - ``record Foo ↦ {}``

Variant declarations
====================

.. list-table::
   :widths: 10 15
   :header-rows: 1

   * - DAML declaration
     - DAML-LF translation
   * - ``data Foo = Bar Int | Baz Text``
     - ``variant Foo ↦ Bar Int64 | Baz Text``
   * - ``data Foo a = Bar a | Baz Text``
     - ``variant Foo a ↦ Bar a | Baz Text``
   * - ``data Foo = Bar Unit | Baz Text``
     - ``variant Foo ↦ Bar Unit | Baz Text``
   * - ``data Foo = Bar Unit | Baz``
     - ``variant Foo ↦ Bar Unit | Baz Unit``
   * - ``data Foo a = Bar | Baz``
     - ``variant Foo a ↦ Bar Unit | Baz Unit``
   * - ``data Foo = Foo Int``
     - ``variant Foo ↦ Foo Int64``
   * - ``data Foo = Bar Int``
     - ``variant Foo ↦ Bar Int64``
   * - ``data Foo = Foo ()``
     - ``variant Foo ↦ Foo Unit``
   * - ``data Foo = Bar ()``
     - ``variant Foo ↦ Bar Unit``
   * - ``data Foo = Bar { bar: Int } | Baz Text``
     - ``variant Foo ↦ Bar Foo.Bar | Baz Text``, ``record Foo.Bar ↦ { bar: Int64 }``
   * - ``data Foo = Foo { foo: Int } | Baz Text``
     - ``variant Foo ↦ Foo Foo.Foo | Baz Text``, ``record Foo.Foo ↦ { foo: Int64 }``
   * - ``data Foo = Bar { bar1: Int; bar2: Decimal } | Baz Text``
     - ``variant Foo ↦ Bar Foo.Bar | Baz Text``, ``record Foo.Bar ↦ { bar1: Int64; bar2: Decimal }``
   * - ``data Foo = Bar { bar1: Int; bar2: Decimal } | Baz { baz1: Text; baz2: Date }``
     - ``data Foo ↦ Bar Foo.Bar | Baz Foo.Baz``, ``record Foo.Bar ↦ { bar1: Int64; bar2: Decimal }``, ``record Foo.Baz ↦ { baz1: Text; baz2: Date }``

Enum declarations
=================

.. list-table::
   :widths: 10 15
   :header-rows: 1

   * - DAML declaration
     - DAML-LF declaration
   * - ``data Foo = Bar | Baz``
     - ``enum Foo ↦ Bar | Baz``
   * - ``data Color = Red | Green | Blue``
     - ``enum Color ↦ Red | Green | Blue``

Banned declarations
===================

There are two gotchas to be aware of: things you might expect to be able to do in DAML that you can't because of DAML-LF.

The first: a single constructor data type must be made unambiguous as to whether it is a record or a variant type. Concretely, the data type declaration ``data Foo = Foo`` causes a compile-time error, because it is unclear whether it is declaring a record or a variant type.

To fix this, you must make the distinction explicitly. Write ``data Foo = Foo {}`` to declare a record type with no fields, or ``data Foo = Foo ()`` for a variant with a single constructor taking unit argument.

The second gotcha is that a constructor in a data type declaration can have at most one unlabelled argument type. This restriction is so that we can provide a straight-forward encoding of DAML-LF types in a variety of client languages.

.. list-table::
   :widths: 10 15
   :header-rows: 1

   * - Banned declaration
     - Workaround
   * - ``data Foo = Foo``
     - ``data Foo = Foo {}`` to produce ``record Foo ↦ {}`` OR ``data Foo = Foo ()`` to produce ``variant Foo ↦ Foo Unit``
   * - ``data Foo = Bar``
     - ``data Foo = Bar {} to produce record Foo ↦ {}`` OR ``data Foo = Bar () to produce variant Foo ↦ Bar Unit``
   * - ``data Foo = Foo Int Text``
     - Name constructor arguments using a record declaration, for example ``data Foo = Foo { x: Int; y: Text }``
   * - ``data Foo = Bar Int Text``
     - Name constructor arguments using a record declaration, for example ``data Foo = Bar { x: Int; y: Text }``
   * - ``data Foo = Bar | Baz Int Text``
     - Name arguments to the Baz constructor, for example ``data Foo = Bar | Baz { x: Int; y: Text }``

Type synonyms
*************

:ref:`Type synonyms <daml-ref-type-synonyms>` (starting with the ``type`` keyword) are eliminated during conversion to DAML-LF. The body of the type synonym is inlined for all occurrences of the type synonym name.

For example, consider the following DAML type declarations.

.. literalinclude:: code-snippets/LfTranslation.daml
   :language: daml
   :start-after: -- start code snippet: type synonyms
   :end-before: -- end code snippet: type synonyms

The ``Username`` type is eliminated in the DAML-LF translation, as follows:

.. code-block:: none

	 record User ↦ { name: Text }

Template types
**************

A :ref:`template declaration <daml-ref-template-name>` in DAML results in one or more data type declarations behind the scenes. These data types, detailed in this section, are not written explicitly in the DAML program but are created by the compiler.

They are translated to DAML-LF using the same rules as for record declarations above.

These declarations are all at the top level of the module in which the template is defined.

Template data types
===================

Every contract template defines a record type for the parameters of the contract. For example, the template declaration:

.. literalinclude:: code-snippets/LfTranslation.daml
   :language: daml
   :start-after: -- start code snippet: template data types
   :end-before: -- end code snippet: template data types

results in this record declaration:

.. literalinclude:: code-snippets/LfResults.daml
   :language: daml
   :start-after: -- start snippet: data from template
   :end-before: -- end snippet: data from template

This translates to the DAML-LF record declaration:

.. code-block:: none

	record Iou ↦ { issuer: Party; owner: Party; currency: Text; amount: Decimal }

Choice data types
=================

Every choice within a contract template results in a record type for the parameters of that choice. For example, let’s suppose the earlier ``Iou`` template has the following choices:

.. literalinclude:: code-snippets/LfTranslation.daml
   :language: daml
   :start-after: -- start code snippet: choice data types
   :end-before: -- end code snippet: choice data types

This results in these two record types:

.. literalinclude:: code-snippets/LfResults.daml
   :language: daml
   :start-after: -- start snippet: data from choices
   :end-before: -- end snippet: data from choices

Whether the choice is consuming or nonconsuming is irrelevant to the data type declaration. The data type is a record even if there are no fields.

These translate to the DAML-LF record declarations:

.. code-block:: none

	record DoNothing ↦ {}
	record Transfer ↦ { newOwner: Party }

Names with special characters
*****************************

All names in DAML—of types, templates, choices, fields, and variant data constructors—are translated to the more restrictive rules of DAML-LF.  ASCII letters, digits, and ``_`` underscore are unchanged in DAML-LF; all other characters must be mangled in some way, as follows:

- ``$`` changes to ``$$``,
- Unicode codepoints less than 65536 translate to ``$uABCD``, where ``ABCD`` are exactly four (zero-padded) hexadecimal digits of the codepoint in question, using only lowercase ``a-f``, and
- Unicode codepoints greater translate to ``$UABCD1234``, where ``ABCD1234`` are exactly eight (zero-padded) hexadecimal digits of the codepoint in question, with the same ``a-f`` rule.

.. list-table::
   :widths: 10 15
   :header-rows: 1

   * - DAML name
     - DAML-LF identifier
   * - ``Foo_bar``
     - ``Foo_bar``
   * - ``baz'``
     - ``baz$u0027``
   * - ``:+:``
     - ``$u003a$u002b$u003a``
   * - ``naïveté``
     - ``na$u00efvet$u00e9``
   * - ``:🙂:``
     - ``$u003a$U0001f642$u003a``
