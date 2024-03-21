// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.value.Record.toJavaProto
import com.daml.ledger.api.v1.value.{Record, Value}
import com.daml.ledger.test.java.model.test._

import java.math.BigDecimal
import java.util.{List => JList}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

class TransactionServiceArgumentsIT extends LedgerTestSuite {
  import ClearIdsImplicits._
  import CompanionImplicits._

  test(
    "TXCreateWithAnyType",
    "Creates should not have issues dealing with any type of argument",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = new ParameterShowcase(
      party,
      42L,
      new BigDecimal("47.0000000000"),
      "some text",
      true,
      TimestampConversion.MIN,
      new NestedOptionalInteger(new optionalinteger.SomeInteger(-1L)),
      JList.of(0, 1, 2, 3),
      Some("some optional text").toJava,
    )
    val create = ledger.submitAndWaitRequest(party, template.create.commands)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val transaction = transactionResponse.getTransaction
      val contract = assertSingleton("CreateWithAnyType", createdEvents(transaction))
      assertEquals(
        "CreateWithAnyType",
        contract.getCreateArguments.clearValueIds,
        Record.fromJavaProto(template.toValue.toProtoRecord),
      )
    }
  })

  test(
    "TXExerciseWithAnyType",
    "Exercise should not have issues dealing with any type of argument",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = new ParameterShowcase(
      party,
      42L,
      new BigDecimal("47.0000000000"),
      "some text",
      true,
      TimestampConversion.MIN,
      new NestedOptionalInteger(new optionalinteger.SomeInteger(-1L)),
      List(0L, 1L, 2L, 3L).map(long2Long).asJava,
      Some("some optional text").toJava,
    )
    val choice1 = new Choice1(
      template.integer,
      new BigDecimal("37.0000000000"),
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
      )(ParameterShowcase.COMPANION)
      tree <- ledger.exercise(party, parameterShowcase.exerciseChoice1(choice1))
    } yield {
      val contract = assertSingleton("ExerciseWithAnyType", exercisedEvents(tree))
      assertEquals(
        "ExerciseWithAnyType",
        clearIds(contract.getChoiceArgument),
        Value.fromJavaProto(choice1.toValue.toProto),
      )
    }
  })

  test(
    "TXVeryLongList",
    "Accept a submission with a very long list (10,000 items)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val n = 10000
    val veryLongList = List(List.iterate(0L, n)(_ + 1): _*).map(long2Long).asJava
    val template = new ParameterShowcase(
      party,
      42L,
      new BigDecimal("47.0000000000"),
      "some text",
      true,
      TimestampConversion.MIN,
      new NestedOptionalInteger(new optionalinteger.SomeInteger(-1L)),
      veryLongList,
      Some("some optional text").toJava,
    )
    val create = ledger.submitAndWaitRequest(party, template.create.commands)
    for {
      transactionResponse <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val transaction = transactionResponse.getTransaction
      val contract = assertSingleton("VeryLongList", createdEvents(transaction))
      assertEquals(
        "VeryLongList",
        toJavaProto(contract.getCreateArguments.clearValueIds),
        template.toValue.toProtoRecord,
      )
    }
  })

  test(
    "TXNoReorder",
    "Don't reorder fields in data structures of choices",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy: Dummy.ContractId <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(
        party,
        dummy.exerciseWrapWithAddress(new Address("street", "city", "state", "zip")),
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
