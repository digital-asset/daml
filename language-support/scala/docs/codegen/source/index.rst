.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Scala CodeGen
==================

Terminology
-----------

=================== ===========
Term                Description
=================== ===========
Ledger              In the context of this document, a distributed ledger — database that is consensually shared and synchronized across a network spread across multiple sites, institutions, or geographies
DAML                Digital Asset’s proprietary language for modeling financial products and processes
Scala               A statically-typed programming language that combines object-oriented and functional programming in one concise, high-level language
ADT                 Algebraic data type
Type class          Powerful and flexible concept that adds ad-hoc polymorphism to Scala
CodeGen             Code Generator, a utility that takes a DAML file as an input and produces Scala bindings for Contract Templates, Contracts, and Choices described in the DAML file
Party               In DAML, an entity participating on the ledger
Contract Template   Reusable blueprint for a DAML contract, specifying fields and choices
Contract            An instance of a contract template
Choice              In a contract, an action/right  that can be exercised by a party
Exercise            Put into effect the right specified in a contract
=================== ===========

Overview
--------

Scala CodeGen generates Scala classes from DAML models. The generated Scala code provides a type safe way of creating contracts and exercising contract choices.


Example
-------

For example the following DAML contract template:

.. DamlVersion 1.0
.. ExcludeFromDamlParsing
.. code-block:: daml

    template CallablePayout
      with
        giver: Party
        receiver: Party
      where
        signatory giver
        controller receiver can
          Call
            returning ContractId PayOut
            to create PayOut with receiver; giver
          Transfer with newReceiver: Party
            returning ContractId CallablePayout
            to create this with receiver = newReceiver

will produce Scala code (some implementation details removed for display purposes):

.. code-block:: scala

    import com.daml.ledger.client.binding.{
      Primitive, Template, TemplateCompanion, Value,
      ValueRef, ValueRefCompanion
    }
    import com.daml.ledger.client.binding.Primitive.{
      ChoiceId, ContractId, Party, Update
    }
    package com.daml.sample.Main {

      final case class CallablePayout(giver: Party,
                                      receiver: Party)
          extends Template[CallablePayout]

      object CallablePayout
          extends TemplateCompanion[CallablePayout]
          with ((Party, Party) => CallablePayout) {
        override val id = ` templateId`(
          packageId = "e6a98a45832f0d7060c64f41909c181f2c94bb156232fdde869ca4161cac29a8",
          name = "Main.CallablePayout")

        final implicit class `CallablePayout syntax`(
            private val id: ContractId[CallablePayout])
            extends AnyVal {
          def exerciseCall(actor: Party): Update[
            ContractId[Main.PayOut]] = ???

          def exerciseTransfer(actor: Party,
                               $choice_arg: Transfer)
            : Update[ContractId[CallablePayout]] = ???

        }

        override val consumingChoices: Set[ChoiceId] =
          ChoiceId.subst(Set("Call", "Transfer"))

        final case class Transfer(newReceiver: Party)
            extends ValueRef

        object Transfer
            extends ValueRefCompanion
            with (Party => Transfer) {
          implicit val `Transfer Value`: Value[Transfer] = ???
        }
      }
    }

The following sections describe what is being generated and how to use the generated Scala code. All code snippets below assume the following global imports:

.. code-block:: scala

    import com.daml.ledger.client.binding.{Primitive => P}
    import com.daml.sample.Main.PayOut

``CallablePayout`` case class is the representation of the contract template:

.. code-block:: scala

    final case class CallablePayout(giver: Party,
                                    receiver: Party)
        extends Template[CallablePayout] {
      ...
    }

Create an instance of this contract like this:

.. code-block:: scala

    import com.daml.sample.Main.CallablePayout

    val createCommand: P.Update[P.ContractId[CallablePayout]] =
      CallablePayout(giver = alice, receiver = bob).create
    sendCommand(createCommand)

.. note::
    Please keep in mind that creating a contract with ``create`` method does not automatically send ``createCommand`` to the ledger. Implementation of ``sendCommand`` method is not covered here.

Below is a template companion for the ``CallablePayout`` template. It contains:

 - a type class instance that knows how to serialize and deserialize an instance of ``CallablePayout`` contract
 - a value class ```CallablePayout syntax``` that contains extension methods to execute contract choices
 - a case class ``Transfer`` that represents arguments for ``Transfer`` choice
 - a type class instance ```Transfer Value``` that knows how to serialize and deserialize an instance of ``Transfer`` class

.. code-block:: scala

    object CallablePayout
        extends ` lfdomainapi`.TemplateCompanion[CallablePayout]

To exercise a ``Call`` choice on a contract ID that was received from the ledger, use the ``exerciseCall`` method with an ``actor`` argument specifying the party that wants to exercise the choice:

.. code-block:: scala

    import com.daml.sample.Main.CallablePayout

    val givenContractId: P.ContractId[CallablePayout] = receiveContractIdFromTheLedger
    val exerciseCommand: P.Update[P.ContractId[PayOut]] =
      givenContractId.exerciseCall(actor = alice)
    sendCommand(exerciseCommand)

.. note::
    Please notice that ``exerciseCommand`` has to be submitted to the ledger. Implementation of ``sendCommand`` method is not covered here.

To exercise ``Transfer`` choice, we need to use ``exerciseTransfer`` method, passing ``actor`` and choice arguments. In the example below party ``bob`` transfers its rights to ``charlie``:

.. code-block:: scala

    import com.daml.sample.Main.CallablePayout

    val givenContractId: P.ContractId[CallablePayout] = receiveContractIdFromTheLedger
    val exerciseCommand: P.Update[P.ContractId[CallablePayout]] =
      givenContractId.exerciseTransfer(actor = bob, newReceiver = charlie)
    sendCommand(exerciseCommand)

If using Scala 2.12, you should add the ``-Xsource:2.13`` option to ``scalacOptions`` to include relevant compiler bugfixes.

Download the entire example:

- :download:`CallablePayout.scala<leo/CallablePayout.scala>`
- :download:`CodeGenExampleSpec.scala<leo/CodeGenExampleSpec.scala>`

