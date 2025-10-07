// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, TestConstraints}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, Update}
import com.daml.ledger.javaapi.data.{Command, DamlRecord, ExerciseCommand, Identifier}
import com.daml.ledger.test.java.semantic.interface$.T
import com.daml.ledger.test.java.semantic.{interface1, interface2}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import java.util.List as JList
import scala.jdk.CollectionConverters.*

class InterfaceIT extends LedgerTestSuite {
  implicit val tCompanion: ContractCompanion.WithoutKey[T.Contract, T.ContractId, T] =
    T.COMPANION

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
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new T(party))
      tree <- ledger.exercise(party, t.exerciseMyArchive())
    } yield {
      val events = exercisedEvents(tree)
      assertLength(s"1 successful exercise", 1, events).discard
      assertEquals(events.head.interfaceId, None)
      assertEquals(events.head.getExerciseResult.getText, "Interface.T")
    }
  })

  test(
    "ExerciseInterfaceSuccess",
    "Success and set interfaceId in output event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new T(party))
      tree <- ledger.exercise(party, t.toInterface(interface1.I.INTERFACE).exerciseMyArchive())
    } yield {
      val events = exercisedEvents(tree)
      assertLength(s"1 successful exercise", 1, events).discard
      assertEquals(events.head.interfaceId, Some(interface1.I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1))
      assertEquals(events.head.getExerciseResult.getText, "Interface1.I")
    }
  })

  test(
    "ExerciseInterfaceByTemplateFail",
    "Cannot exercise an interface choice using templateId",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("Problem creating faulty JSON from a faulty GRPC call"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new T(party))
      failure <- ledger
        .submitAndWaitForTransaction(
          ledger.submitAndWaitForTransactionRequest(
            party,
            useWrongId(t.toInterface(interface1.I.INTERFACE).exerciseChoiceI1(), T.TEMPLATE_ID),
          )
        )
        .mustFail("unknown choice")
    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
        Some("unknown choice ChoiceI1"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ExerciseInterfaceByRequiringFail",
    "Cannot exercise an interface choice using requiring templateId",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("Problem creating faulty JSON from a faulty GRPC call"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      t <- ledger.create(party, new T(party))
      failure <- ledger
        .submitAndWaitForTransaction(
          ledger.submitAndWaitForTransactionRequest(
            party,
            useWrongId(
              t.toInterface(interface1.I.INTERFACE).exerciseChoiceI1(),
              interface2.I.TEMPLATE_ID,
            ),
          )
        )
        .mustFail("unknown choice")
    } yield {
      assertGrpcError(
        failure,
        CommandExecutionErrors.Preprocessing.PreprocessingFailed,
        Some("unknown choice ChoiceI1"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

}
