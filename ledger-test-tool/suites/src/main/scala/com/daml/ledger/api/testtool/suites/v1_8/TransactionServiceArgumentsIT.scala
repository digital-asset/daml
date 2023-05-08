// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Value.encode
import com.daml.ledger.test.model.Test.Choice1._
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test.ParameterShowcase._
import com.daml.ledger.test.model.Test._

import scala.collection.immutable.Seq

class TransactionServiceArgumentsIT extends LedgerTestSuite {
  test(
    "TXCreateWithAnyType",
    "Creates should not have issues dealing with any type of argument",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = ParameterShowcase(
      operator = party,
      integer = 42L,
      decimal = BigDecimal("47.0000000000"),
      text = "some text",
      bool = true,
      time = Primitive.Timestamp.MIN,
      nestedOptionalInteger = NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
      integerList = Primitive.List(0L, 1L, 2L, 3L),
      optionalText = Primitive.Optional("some optional text"),
    )
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val transaction = transactionResponse.getTransaction
      val contract = assertSingleton("CreateWithAnyType", createdEvents(transaction))
      assertEquals("CreateWithAnyType", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXExerciseWithAnyType",
    "Exercise should not have issues dealing with any type of argument",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = ParameterShowcase(
      operator = party,
      integer = 42L,
      decimal = BigDecimal("47.0000000000"),
      text = "some text",
      bool = true,
      time = Primitive.Timestamp.MIN,
      nestedOptionalInteger = NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
      integerList = Primitive.List(0L, 1L, 2L, 3L),
      optionalText = Primitive.Optional("some optional text"),
    )
    val choice1 = Choice1(
      template.integer,
      BigDecimal("37.0000000000"),
      template.text,
      template.bool,
      template.time,
      template.nestedOptionalInteger,
      template.integerList,
      template.optionalText,
    )
    for {
      parameterShowcase <- ledger.create(
        party,
        template,
      )
      tree <- ledger.exercise(party, parameterShowcase.exerciseChoice1(choice1))
    } yield {
      val contract = assertSingleton("ExerciseWithAnyType", exercisedEvents(tree))
      assertEquals("ExerciseWithAnyType", contract.getChoiceArgument, encode(choice1))
    }
  })

  test(
    "TXVeryLongList",
    "Accept a submission with a very long list (10,000 items)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val n = 10000
    val veryLongList = Primitive.List(List.iterate(0L, n)(_ + 1): _*)
    val template = ParameterShowcase(
      operator = party,
      integer = 42L,
      decimal = BigDecimal("47.0000000000"),
      text = "some text",
      bool = true,
      time = Primitive.Timestamp.MIN,
      nestedOptionalInteger = NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
      integerList = veryLongList,
      optionalText = Primitive.Optional("some optional text"),
    )
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val transaction = transactionResponse.getTransaction
      val contract = assertSingleton("VeryLongList", createdEvents(transaction))
      assertEquals("VeryLongList", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXNoReorder",
    "Don't reorder fields in data structures of choices",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(
        party,
        dummy.exerciseWrapWithAddress(Address("street", "city", "state", "zip")),
      )
    } yield {
      val contract = assertSingleton("Contract in transaction", createdEvents(tree))
      val fields = assertLength("Fields in contract", 2, contract.getCreateArguments.fields)
      assertEquals(
        "NoReorder",
        fields.flatMap(_.getValue.getRecord.fields).map(_.getValue.getText).zipWithIndex,
        Seq("street" -> 0, "city" -> 1, "state" -> 2, "zip" -> 3),
      )
    }
  })
}
