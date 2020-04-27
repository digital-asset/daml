.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

How DAML types are translated to protobuf
#########################################

This page gives an overview and reference on how DAML types and contracts are represented by the Ledger API as protobuf messages, most notably:

- in the stream of transactions from the :ref:`com.daml.ledger.api.v1.transactionservice`
- as payload for :ref:`com.daml.ledger.api.v1.createcommand` and :ref:`com.daml.ledger.api.v1.exercisecommand` sent to :ref:`com.daml.ledger.api.v1.commandsubmissionservice` and :ref:`com.daml.ledger.api.v1.commandservice`.

The DAML code in the examples below is written in DAML *1.1*.

Notation
********

The notation used on this page for the protobuf messages is the same as you get if you invoke ``protoc --decode=Foo < some_payload.bin``. To illustrate the notation, here is a simple definition of the messages ``Foo`` and ``Bar``:

.. literalinclude:: ../code-snippets/notation.proto
	:language: protobuf
	:start-after: // start snippet
	:end-before: // end snippet

A particular value of ``Foo`` is then represented by the Ledger API in this way:

.. literalinclude:: ../code-snippets/notation.payload

The name of messages is added as a comment after the opening curly brace.

Records and primitive types
***************************

Records or product types are translated to :ref:`com.daml.ledger.api.v1.record`. Here's an example DAML record type that contains a field for each primitive type:

.. literalinclude:: ../code-snippets/Types.daml
	:language: daml
	:lines: 9-18

And here's an example of creating a value of type `MyProductType`:

.. literalinclude:: ../code-snippets/Types.daml
	:language: daml
	:lines: 29,31,33-41

For this data, the respective data on the Ledger API is shown below. Note that this value would be enclosed by a particular contract containing a field of type `MyProductType`. See `Contract templates`_ for the translation of DAML contracts to the representation by the Ledger API.

.. literalinclude:: ../code-snippets/records.payload

Variants
********

Variants or sum types are types with multiple constructors. This example defines a simple variant type with two constructors:

.. literalinclude:: ../code-snippets/Types.daml
	:language: daml
	:lines: 20-21

The constructor ``MyConstructor1`` takes a single parameter of type ``Integer``, whereas the constructor ``MyConstructor2`` takes a record with two fields as parameter. The snippet below shows how you can create values with either of the constructors.

.. literalinclude:: ../code-snippets/Types.daml
	:language: daml
	:lines: 43-44

Similar to records, variants are also enclosed by a contract, a record, or another variant.

The snippets below shows the value of ``mySum1`` and ``mySum2`` respectively as they would be transmitted on the Ledger API within a contract.

.. literalinclude:: ../code-snippets/MySumType.payload
	:lines: 1-12
	:caption: mySum1

.. literalinclude:: ../code-snippets/MySumType.payload
	:lines: 14-38
	:caption: mySum2

Contract templates
******************

Contract templates are represented as records with the same identifier as the template.

This first example template below contains only the signatory party and a simple choice to exercise:

.. literalinclude:: ../code-snippets/Templates.daml
    :language: daml
    :lines: 6-18

Creating a contract
===================

Creating contracts is done by sending a :ref:`com.daml.ledger.api.v1.createcommand` to the :ref:`com.daml.ledger.api.v1.commandsubmissionservice` or the :ref:`com.daml.ledger.api.v1.commandservice`. The message to create a `MySimpleTemplate` contract with *Alice* being the owner is shown below:

.. literalinclude:: ../code-snippets/CreateMySimpleTemplate.payload

Receiving a contract
====================

Contracts are received from the :ref:`com.daml.ledger.api.v1.transactionservice` in the form of a :ref:`com.daml.ledger.api.v1.createdevent`. The data contained in the event corresponds to the data that was used to create the contract.

.. literalinclude:: ../code-snippets/CreatedEventMySimpleTemplate.payload

Exercising a choice
===================

A choice is exercised by sending an :ref:`com.daml.ledger.api.v1.exercisecommand`. Taking the same contract template again, exercising the choice ``MyChoice`` would result in a command similar to the following:

.. literalinclude:: ../code-snippets/ExerciseMySimpleTemplate.payload

If the template specifies a key, the :ref:`com.daml.ledger.api.v1.exercisebykeycommand` can be used. It works in a similar way as :ref:`com.daml.ledger.api.v1.exercisecommand`, but instead of specifying the contract identifier you have to provide its key. The example above could be rewritten as follows:

.. literalinclude:: ../code-snippets/ExerciseByKeyMySimpleTemplate.payload
