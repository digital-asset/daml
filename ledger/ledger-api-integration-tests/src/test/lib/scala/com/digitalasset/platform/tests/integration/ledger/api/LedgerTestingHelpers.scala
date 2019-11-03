// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.{MockMessages => M}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.{CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement
import com.digitalasset.platform.apitesting.LedgerContext
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code
import org.scalatest.concurrent.Waiters
import org.scalatest.{Assertion, Inside, Matchers, OptionValues}
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerTestingHelpers(
    submitCommand: SubmitRequest => Future[Completion],
    context: LedgerContext,
    timeoutScaleFactor: Double = 1.0)(implicit mat: ActorMaterializer)
    extends Matchers
    with Waiters
    with Inside
    with OptionValues {

  implicit private val ec: ExecutionContextExecutor = mat.executionContext

  val submitSuccessfully: SubmitRequest => Future[Assertion] =
    submitCommand.andThen(_.map(assertCompletionIsSuccessful))

  private def assertCompletionIsSuccessful(completion: Completion): Assertion = {
    inside(completion) { case c => c.getStatus should have('code (0)) }
  }

  def createdEventsIn(transaction: Transaction): Seq[CreatedEvent] =
    transaction.events.map(_.event).collect {
      case Created(createdEvent) =>
        createdEvent
    }

  def findCreatedEventIn(
      contractCreationTx: Transaction,
      templateToLookFor: Identifier): CreatedEvent = {
    // for helpful scalatest error message
    createdEventsIn(contractCreationTx).flatMap(_.templateId.toList) should contain(
      templateToLookFor)
    createdEventsIn(contractCreationTx).find(_.templateId.contains(templateToLookFor)).value
  }

  def assertNoArchivedEvents[T](tx: Transaction) =
    tx.events.map(_.event).collect {
      case Archived(archivedEvent) => archivedEvent
    } shouldBe empty

  def getHead[T](elements: Iterable[T]): T = {
    elements should have size 1
    elements.headOption.value
  }

  def recordWithArgument(original: Record, fieldToInclude: RecordField): Record = {
    original.update(_.fields.modify(recordFieldsWithArgument(_, fieldToInclude)))
  }

  private def recordFieldsWithArgument(
      originalFields: Seq[RecordField],
      fieldToInclude: RecordField): Seq[RecordField] = {
    var replacedAnElement: Boolean = false
    val updated = originalFields.map { original =>
      if (original.label == fieldToInclude.label) {
        replacedAnElement = true
        fieldToInclude
      } else {
        original
      }
    }
    if (replacedAnElement) updated else originalFields :+ fieldToInclude

  }

  def failingCreate(
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      submitRequestWithId(commandId, submitter)
        .update(
          _.commands.commands :=
            List(CreateCommand(Some(template), Some(arg)).wrap)
        ),
      code,
      pattern
    )

  def assertCommandFailsWithCode(
      submitRequest: SubmitRequest,
      expectedErrorCode: Code,
      expectedMessageSubString: String): Future[Assertion] = {
    for {
      completion <- submitCommand(submitRequest)
    } yield {
      completion.getStatus should have('code (expectedErrorCode.value))
      completion.getStatus.message should include(expectedMessageSubString)
    }
  }

  def submitRequestWithId(commandId: String, submitter: String): SubmitRequest =
    M.submitRequest.update(
      _.commands.party := submitter,
      _.commands.commandId := commandId,
      _.commands.ledgerId := context.ledgerId.unwrap)

  // Exercise a choice that is supposed to fail.
  def failingExercise(
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      submitRequestWithId(commandId, submitter)
        .update(
          _.commands.commands :=
            List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap)
        ),
      code,
      pattern
    )

  def listenForCompletionAsApplication(
      applicationId: String,
      requestingParty: String,
      offset: LedgerOffset,
      commandIdToListenFor: String) = {
    context.commandClient(applicationId = applicationId).flatMap { commandClient =>
      commandClient
        .completionSource(List(requestingParty), offset)
        .collect {
          case CompletionStreamElement.CompletionElement(completion)
              if completion.commandId == commandIdToListenFor =>
            completion
        }
        .take(1)
        .takeWithin(scaled(3.seconds))
        .runWith(Sink.seq)
        .map(_.headOption)
    }
  }

  override def spanScaleFactor: Double = timeoutScaleFactor
}
