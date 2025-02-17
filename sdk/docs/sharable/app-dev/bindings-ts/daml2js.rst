.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Use the JavaScript Code Generator
#################################

The command ``daml codegen js`` generates JavaScript (and TypeScript) that can be used in conjunction with the `JavaScript Client Libraries </app-dev/bindings-ts/index.html>`_ for interacting with a Daml ledger via the `HTTP JSON API </json-api/index.html>`_.

Inputs to the command are DAR files. Outputs are JavaScript packages with TypeScript typings containing metadata and types for all Daml packages included in the DAR files.

The generated packages use the library `@daml/types <ts-daml-types_>`_.

Generate and Use Code
*********************

In outline, the command to generate JavaScript and TypeScript typings from Daml is ``daml codegen js -o OUTDIR DAR`` where ``DAR`` is the path to a DAR file (generated via ``daml build``) and ``OUTDIR`` is a directory where you want the artifacts to be written.

Here's a complete example on a project built from the standard "skeleton" template.

.. code-block:: bash
   :linenos:

   daml new my-proj --template skeleton # Create a new project based off the skeleton template
   cd my-proj # Enter the newly created project directory
   daml build  # Compile the project's Daml files into a DAR
   daml codegen js -o daml.js .daml/dist/my-proj-0.0.1.dar # Generate JavaScript packages in the daml.js directory

- On execution of these commands:

  - The directory ``my-proj/daml.js`` contains generated JavaScript packages with TypeScript typings;
  - The files are arranged into directories;
  - One of those directories will be named my-proj-0.0.1 and will contain the definitions corresponding to the Daml files in the project;
  - For example, ``daml.js/my-proj-0.0.1/lib/index.js`` provides access to the definitions for ``daml/Main.daml``;
  - The remaining directories correspond to modules of the Daml standard library;
  - Those directories have numeric names (the names are hashes of the Daml-LF package they are derived from).

To get a quickstart idea of how to use what has been generated, you may wish to jump to the `Templates and choices`_ section and return to the reference material that follows as needed.

Primitive Daml Types: @daml/types
*********************************

To understand the TypeScript typings produced by the code generator, it is helpful to keep in mind this quick review of the TypeScript equivalents of the primitive Daml types provided by @daml/types.

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
   The TypeScript types ``Time``, ``Decimal``, ``Numeric`` and ``Int`` all alias to ``string``. These choices relate to the avoidance of precision loss under serialization over the `json-api <../json-api/index.html>`_.

.. note::
   The TypeScript definition of type ``Optional<τ>`` in the above table might look complicated. It accounts for differences in the encoding of optional values when nested versus when they are not (i.e. "top-level"). For example, ``null`` and ``"foo"`` are two possible values of ``Optional<Text>`` whereas, ``[]`` and ``["foo"]`` are two possible values of type ``Optional<Optional<Text>>`` (``null`` is another possible value, ``[null]`` is **not**).

Daml to TypeScript Mappings
***************************

The mappings from Daml to TypeScript are best explained by example.

Records
=======

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

Variants
========

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

Sum-of-products
===============

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
=====

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
=====================

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

Each template results in the generation of a companion object. Here, is a schematic of the one generated from the ``Iou`` template [2]_.

.. code-block:: typescript
   :linenos:

   const Iou: daml.Template<Iou, undefined> & {
     Archive: daml.Choice<Iou, DA_Internal_Template.Archive, {}, undefined>;
     Transfer: daml.Choice<Iou, Transfer, daml.ContractId<Iou>, undefined>;
   } = {
     /* ... */
   }

.. [2] The ``undefined`` type parameter captures the fact that ``Iou`` has no contract key.

The exact details of these companion objects are not important - think of them as representing "metadata".

What **is** important is the use of the companion objects when creating contracts and exercising choices using the `@daml/ledger <https://github.com/digital-asset/daml/tree/main/language-support/ts/daml-ledger>`_ package. The following code snippet demonstrates their usage.

.. code-block:: typescript
   :linenos:

   import Ledger from  '@daml/ledger';
   import {Iou, Transfer} from /* ... */;

   const ledger = new Ledger(/* ... */);

   // Contract creation; Bank issues Alice a USD $1MM IOU.

   const iouDetails: Iou = {
     issuer: 'Chase',
     owner: 'Alice',
     currency: 'USD',
     amount: 1000000.0,
   };
   const aliceIouCreateEvent = await ledger.create(Iou, iouDetails);
   const aliceIouContractId = aliceIouCreateEvent.contractId;

   // Choice execution; Alice transfers ownership of the IOU to Bob.

   const transferDetails: Transfer = {
     newOwner: 'Bob',
   }
   const [bobIouContractId, _] = await ledger.exercise(Transfer, aliceIouContractId, transferDetails);

Observe on line 14, the first argument to ``create`` is the ``Iou`` companion object and on line 22, the first argument to ``exercise`` is the ``Transfer`` companion object.
