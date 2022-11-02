// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

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
import com.daml.ledger.test.semantic.{Interface1, Interface2, Interface3}
import com.daml.ledger.errors.LedgerApiErrors
import scalaz.Tag

class InterfaceIT extends LedgerTestSuite {

  private[this] val TId = Tag.unwrap(T.id)
  private[this] val I1Id = Tag.unwrap(Interface1.I.id)
  private[this] val I2Id = Tag.unwrap(Interface2.I.id)
  private[this] val I3Id = Tag.unwrap(Interface3.I.id)

  // replace identifier with the wrong identifier for some of these tests
  private[this] def useWrongId[X](
      command: Primitive.Update[X],
      id: Identifier,
  ): Primitive.Update[X] = {
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
      tree <- ledger.exercise(party, t.exerciseMyArchive())
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
      tree <- ledger.exercise(party, t.toInterface[Interface1.I].exerciseMyArchive())
    } yield {
      val events = exercisedEvents(tree)
      assertLength(s"1 successful exercise", 1, events)
      assertEquals(events.head.interfaceId, Some(I1Id))
      assertEquals(events.head.getExerciseResult.getText, "Interface1.I")
    }
  })

  test(
    "ExerciseRetroactiveInterfaceInstanceSuccess",
    "Success and set interfaceId in output event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, T(party))
      tree <-
        ledger
          .exercise(party, t.asInstanceOf[Primitive.ContractId[Interface3.I]].exerciseMyArchive())
    } yield {
      val events = exercisedEvents(tree)
      assertLength(s"1 successful exercise", 1, events)
      assertEquals(events.head.interfaceId, Some(I3Id))
      assertEquals(events.head.getExerciseResult.getText, "Interface3.I")
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
        .exercise(party, useWrongId(t.toInterface[Interface1.I].exerciseChoiceI1(), TId))
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
        .exercise(party, useWrongId(t.toInterface[Interface1.I].exerciseChoiceI1(), I2Id))
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
