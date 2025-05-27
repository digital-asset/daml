.. _component-howtos-application-development-daml-codegen-javascript:

Daml Codegen for JavaScript
===========================

Use the Daml Codegen for JavaScript (``daml codegen js``) to generate JavaScript/TypeScript code representing all Daml data types
defined in a Daml Archive (.dar) file.


The generated code makes it easier to construct types and work with JSON when using the
:brokenref:`JSON Ledger API <TODO(#369): add link to JSON Ledger API>`.

.. TODO: add link to JSON Ledger API tutorial

See :brokenref:`Get started with Canton and the JSON Ledger API <link>` for details on how to use the generated code to
interact with JSON Ledger API. See the sections below for guidance on setting up and invoking the codegen.

Install
-------

Install the Daml Codegen for JavaScript by :brokenref:`installing the Daml Assistant <Install section of Daml Assistant>`.

Configure
---------

To configure the Daml Codegen, choose one of the two following methods:

- **Command line configuration**: Specify all settings directly in the command line.

- **Project file configuration**: Define all settings in the `daml.yaml` file.

Command line configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

To view all available command line configuration options for Daml Codegen for JavaScript, run ``daml codegen js --help`` in your terminal:

.. code-block:: none

      DAR-FILES                DAR files to generate TypeScript bindings for
      -o DIR                   Output directory for the generated packages
      -s SCOPE                 The NPM scope name for the generated packages;
                               defaults to daml.js
      -h,--help                Show this help text



Project file configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

Specify the above settings in the ``codegen`` element of the Daml project file ``daml.yaml``.

Here is an example::

    sdk-version: 3.3.0-snapshot.20250507.0
    name: quickstart
    source: daml
    init-script: Main:initialize
    parties:
      - Alice
      - Bob
      - USD_Bank
      - EUR_Bank
    version: 0.0.1
    exposed-modules:
      - Main
    dependencies:
      - daml-prim
      - daml-stdlib
    codegen:
      js:
        output-directory: ui/daml.js
        npm-scope: daml.js

Operate
-------

Run the Daml Codegen using project file configuration with::

    $ daml codegen js

or using command line configuration with::

    $ daml codegen js ./.daml/dist/quickstart-0.0.1.dar -o ui/daml.js -s daml.js

References
----------

.. _component-howtos-application-development-daml-codegen-javascript-generated-code:

Generated JavaScript/TypeScript code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _component-howtos-application-development-daml-codegen-javascript-primitive-types:

Daml primitives to TypeScript
"""""""""""""""""""""""""""""

Daml built-in types are translated to the following equivalent types in TypeScript:
The TypeScript equivalents of the primitive Daml types are provided by the @daml/types
:brokenref:`JavaScript client library <link>`.

.. TODO: add link to JavaScript client library

**Interfaces**:

- ``Template<T extends object, K = unknown>``
- ``Choice<T extends object, C, R, K = unknown>``

**Types**:

+-------------------+--------------------+----------------------------------+
| Daml              | TypeScript         | TypeScript definition            |
+===================+====================+==================================+
| ``()``            | ``Unit``           | ``{}``                           |
+-------------------+--------------------+----------------------------------+
| ``Bool``          | ``Bool``           | ``boolean``                      |
+-------------------+--------------------+----------------------------------+
| ``Int``           | ``Int``            | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``Decimal``       | ``Decimal``        | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``Numeric ν``     | ``Numeric``        | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``Text``          | ``Text``           | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``Time``          | ``Time``           | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``Party``         | ``Party``          | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``[τ]``           | ``List<τ>``        | ``τ[]``                          |
+-------------------+--------------------+----------------------------------+
| ``Date``          | ``Date``           | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``ContractId τ``  | ``ContractId<τ>``  | ``string``                       |
+-------------------+--------------------+----------------------------------+
| ``Optional τ``    | ``Optional<τ>``    | ``null | (null extends τ ?``     |
|                   |                    | ``[] | [Exclude<τ, null>] : τ)`` |
+-------------------+--------------------+----------------------------------+
| ``TextMap τ``     | ``TextMap<τ>``     | ``{ [key: string]: τ }``         |
+-------------------+--------------------+----------------------------------+
| ``(τ₁, τ₂)``      | ``Tuple₂<τ₁, τ₂>`` | ``{_1: τ₁; _2: τ₂}``             |
+-------------------+--------------------+----------------------------------+

.. note::
   The types given in the "TypeScript" column are defined in @daml/types.

.. note::
   For *n*-tuples where *n ≥ 3*, representation is analogous with the pair case (the last line of the table).

.. note::
   The TypeScript types ``Time``, ``Decimal``, ``Numeric`` and ``Int`` all alias to ``string``. These choices relate to
   the avoidance of precision loss under serialization over the :brokenref:`JSON Ledger API <link>`.

.. note::
   The TypeScript definition of type ``Optional<τ>`` in the above table might look complicated. It accounts for differences in the encoding of optional values when nested versus when they are not (i.e. "top-level"). For example, ``null`` and ``"foo"`` are two possible values of ``Optional<Text>`` whereas, ``[]`` and ``["foo"]`` are two possible values of type ``Optional<Optional<Text>>`` (``null`` is another possible value, ``[null]`` is **not**).

Generated TypeScript mappings
"""""""""""""""""""""""""""""

The mappings from user-defined data types in Daml to TypeScript are best explained by example.

Records (a.k.a product types)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Daml, we might model a person like this.

.. code-block:: daml
   :linenos:

   data Person =
     Person with
       name: Text
       party: Party
       age: Int

Given the above definition, the generated TypeScript code will be as follows.

.. code-block:: typescript
   :linenos:

   type Person = {
     name: string;
     party: daml.Party;
     age: daml.Int;
   }

Variants (a.k.a sum types)
~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a Daml type for a language of additive expressions.

.. code-block:: daml
   :linenos:

   data Expr a =
       Lit a
     | Var Text
     | Add (Expr a, Expr a)

In TypeScript, it is represented as a `discriminated union <https://www.typescriptlang.org/docs/handbook/advanced-types.html#discriminated-unions>`_.

.. code-block:: typescript
   :linenos:

   type Expr<a> =
     |  { tag: 'Lit'; value: a }
     |  { tag: 'Var'; value: string }
     |  { tag: 'Add'; value: {_1: Expr<a>, _2: Expr<a>} }

Sum of products
~~~~~~~~~~~~~~~

Let's slightly modify the ``Expr a`` type of the last section into the following.

.. code-block:: daml
   :linenos:

   data Expr a =
       Lit a
     | Var Text
     | Add {lhs: Expr a, rhs: Expr a}

Compared to the earlier definition, the ``Add`` case is now in terms of a record with fields ``lhs`` and ``rhs``. This renders in TypeScript like so.

.. code-block:: typescript
   :linenos:

   type Expr<a> =
     |  { tag: 'Lit2'; value: a }
     |  { tag: 'Var2'; value: string }
     |  { tag: 'Add'; value: Expr.Add<a> }

   namespace Expr {
     type Add<a> = {
       lhs: Expr<a>;
       rhs: Expr<a>;
     }
   }

The thing to note is how the definition of the ``Add`` case has given rise to a record type definition ``Expr.Add``.

Enums
~~~~~

Given a Daml enumeration like this,

.. code-block:: daml
   :linenos:

   data Color = Red | Blue | Yellow

the generated TypeScript will consist of a type declaration and the definition of an associated companion object.

.. code-block:: typescript
   :linenos:

   type Color = 'Red' | 'Blue' | 'Yellow'

   const Color = {
     Red: 'Red',
     Blue: 'Blue',
     Yellow: 'Yellow',
     keys: ['Red','Blue','Yellow'],
   } as const;

Templates and Choices
~~~~~~~~~~~~~~~~~~~~~

Here is a Daml template of a basic 'IOU' contract.

.. code-block:: daml
   :linenos:

   template Iou
     with
       issuer: Party
       owner: Party
       currency: Text
       amount: Decimal
     where
       signatory issuer
       choice Transfer: ContractId Iou
         with
           newOwner: Party
         controller owner
         do
           create this with owner = newOwner

The ``daml codegen js`` command generates types for each of the choices defined on the template as well as the template itself.

.. code-block:: typescript
   :linenos:

   type Transfer = {
     newOwner: daml.Party;
   }

   type Iou = {
     issuer: daml.Party;
     owner: daml.Party;
     currency: string;
     amount: daml.Numeric;
   }

Each template results in the generation of a companion object. Here, is a schematic of the one generated from the
``Iou`` template [2]_.

.. code-block:: typescript
   :linenos:

   const Iou: daml.Template<Iou, undefined> & {
     Archive: daml.Choice<Iou, DA_Internal_Template.Archive, {}, undefined>;
     Transfer: daml.Choice<Iou, Transfer, daml.ContractId<Iou>, undefined>;
   } = {
     /* ... */
   }

.. [2] The ``undefined`` type parameter captures the fact that ``Iou`` has no contract key.

The exact details of these companion objects are not important, think of them as representing metadata.

Use the companion objects to create contracts and exercise choices using the :brokenref:`@daml/ledger <link to @daml/ledger library documentation>` package.
See the :brokenref:`Use contracts and transactions in JavaScript <link>` for details on how to use them and generated
code in general to interact with the JSON Ledger API.

