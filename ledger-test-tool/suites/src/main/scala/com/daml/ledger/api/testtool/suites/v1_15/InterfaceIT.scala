// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

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
import com.daml.ledger.javaapi.data.{Command, DamlRecord, ExerciseCommand, Identifier}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, Update}
import com.daml.ledger.test.java.semantic.interface$._
import com.daml.ledger.test.java.semantic.{interface1, interface2, interface3}

import java.util.{List => JList}
import scala.jdk.CollectionConverters._

class InterfaceIT extends LedgerTestSuite {
  implicit val tCompanion: ContractCompanion.WithKey[T.Contract, T.ContractId, T, String] =
    T.COMPANION

  private[this] val TId = T.TEMPLATE_ID
  private[this] val I1Id = interface1.I.TEMPLATE_ID.toV1
  private[this] val I2Id = interface2.I.TEMPLATE_ID
  private[this] val I3Id = interface3.I.TEMPLATE_ID.toV1

  // replace identifier with the wrong identifier for some of these tests
  private[this] def useWrongId[X](
      update: Update[X],
      id: Identifier,
  ): JList[Command] = {
    val command = update.commands.asScala.head
    val exe = command.asExerciseCommand.get
    val arg = exe.getChoiceArgument.asRecord.get
    JList.of(
      new ExerciseCommand(
        id,
        exe.getContractId,
        exe.getChoice,
        new DamlRecord(arg.getFields),
      )
    )
  }

  test(
    "ExerciseTemplateSuccess",
    "Success but does not set interfaceId in output event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      t <- ledger.create(party, new T(party))
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
      t <- ledger.create(party, new T(party))
      tree <- ledger.exercise(party, t.toInterface(interface1.I.INTERFACE).exerciseMyArchive())
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
      t <- ledger.create(party, new T(party))
      tree <-
        ledger
          .exercise(party, (new interface3.I.ContractId(t.contractId)).exerciseMyArchive())
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
      t <- ledger.create(party, new T(party))
      failure <- ledger
        .submitAndWaitForTransactionTree(
          ledger.submitAndWaitRequest(
            party,
            useWrongId(t.toInterface(interface1.I.INTERFACE).exerciseChoiceI1(), TId),
          )
        )
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
      t <- ledger.create(party, new T(party))
      failure <- ledger
        .submitAndWaitForTransactionTree(
          ledger.submitAndWaitRequest(
            party,
            useWrongId(t.toInterface(interface1.I.INTERFACE).exerciseChoiceI1(), I2Id),
          )
        )
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
