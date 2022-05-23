// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.v1.value.{Identifier, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.semantic.Interface._
import scalaz.Tag

class InterfaceIT extends LedgerTestSuite {

  private[this] val TId = Tag.unwrap(T.id)
  private[this] val I1Id = Tag.unwrap(T.id).copy(moduleName = "Interface1", entityName = "I")
  private[this] val I2Id = Tag.unwrap(T.id).copy(moduleName = "Interface2", entityName = "I")

  // Workaround improper support of scala Codegen TODO(#13349)
  private[this] def fixId[X](command: Primitive.Update[X], id: Identifier): Primitive.Update[X] = {
    val exe = command.command.getExercise
    val arg = exe.getChoiceArgument.getRecord
    command
      .copy(
        command = command.command.withExercise(
          exe.copy(
            templateId = Some(id),
            choiceArgument = Some(Value.of(Value.Sum.Record(arg.copy(recordId = None)))),
          )
        )
      )
      .asInstanceOf[Primitive.Update[X]]
  }

  test(
    "ExerciseTemplateSuccess",
    "Success but does not set interfaceId in output event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, T(party))
      tree <- ledger.exercise(party, x => fixId(t.exerciseMyArchive(x), TId))
    } yield {
      val events = exercisedEvents(tree)
      assertLength(s"1 successful exercise", 1, events)
      assertEquals(events.head.interfaceId, None)
      assertEquals(events.head.getExerciseResult.getText, "Interface.T")
    }
  })

  test(
    "ExerciseInterfaceSuccess",
    "Success and set interfaceId in output event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, T(party))
      tree <- ledger.exercise(party, x => fixId(t.exerciseMyArchive(x), I1Id))
    } yield {
      val events = exercisedEvents(tree)
      assertLength(s"1 successful exercise", 1, events)
      assertEquals(events.head.interfaceId, Some(I1Id))
      assertEquals(events.head.getExerciseResult.getText, "Interface1.I")
    }
  })

  test(
    "ExerciseInterfaceByTemplateFail",
    "Cannot exercise an interface choice using templateId",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, T(party))
      failure <- ledger
        .exercise(party, x => fixId(t.exerciseChoiceI1(x), TId))
        .mustFail("unknown choice")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
        Some("unknown choice ChoiceI1"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExerciseInterfaceByRequiringFail",
    "Cannot exercise an interface choice using requiring templateId",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, T(party))
      failure <- ledger
        .exercise(party, x => fixId(t.exerciseChoiceI1(x), I2Id))
        .mustFail("unknown choice")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
        Some("unknown choice ChoiceI1"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

}
