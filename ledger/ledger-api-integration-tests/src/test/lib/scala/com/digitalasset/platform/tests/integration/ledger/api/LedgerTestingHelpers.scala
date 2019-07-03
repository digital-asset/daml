// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.{MockMessages => M}
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitRequest
}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.{
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand
}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement
import com.digitalasset.platform.apitesting.LedgerContext
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.Waiters
import org.scalatest.{Assertion, Inside, Matchers, OptionValues}
import scalaz.syntax.tag._

import scala.collection.{breakOut, immutable}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerTestingHelpers(
    submitCommand: SubmitRequest => Future[Completion],
    context: LedgerContext,
    timeoutScaleFactor: Double = 1.0)(implicit mat: ActorMaterializer)
    extends Matchers
    with Waiters
    with FutureTimeouts
    with Inside
    with OptionValues {

  private val transactionClient = context.transactionClient

  implicit private val ec: ExecutionContextExecutor = mat.executionContext

  implicit def submitAndWait2SubmitReq(sw: SubmitAndWaitRequest): SubmitRequest =
    SubmitRequest(sw.commands, sw.traceContext)

  val submitSuccessfully: SubmitRequest => Future[Assertion] = req => {
    submitCommand(req).map(assertCompletionIsSuccessful)
  }

  def assertCompletionIsSuccessful(completion: Completion): Assertion = {
    inside(completion) { case c => c.getStatus should have('code (0)) }
  }

  def getAllContracts(parties: Seq[String]) = TransactionFilter(parties.map(_ -> Filters()).toMap)

  def assertCommandFailsWithCode(
      submitRequest: SubmitRequest,
      expectedErrorCode: Code,
      expectedMessageSubString: String,
      ignoreCase: Boolean = false): Future[Assertion] = {
    for {
      completion <- submitCommand(submitRequest)
    } yield {
      completion.getStatus should have('code (expectedErrorCode.value))
      if (ignoreCase)
        completion.getStatus.message.toLowerCase() should include(
          expectedMessageSubString.toLowerCase())
      else completion.getStatus.message should include(expectedMessageSubString)
    }
  }

  def submitAndListenForSingleResultOfCommand(
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter,
      verbose: Boolean = false): Future[Transaction] = {
    for {
      (txEndOffset, transactionId) <- submitAndReturnOffsetAndTransactionId(submitRequest)
      tx <- expectTransaction(transactionFilter, transactionId, txEndOffset, verbose)
    } yield {
      tx
    }
  }

  def expectTransaction(
      transactionFilter: TransactionFilter,
      transactionId: String,
      txEndOffset: LedgerOffset,
      verbose: Boolean) = {
    for {
      tx <- transactionClient
        .getTransactions(
          txEndOffset,
          None,
          transactionFilter,
          verbose
        )
        .filter(_.transactionId == transactionId)
        .take(1)
        .runWith(Sink.head)
    } yield {
      tx
    }
  }

  def submitAndVerifyFilterCantSeeResultOf(
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter): Future[Assertion] = {
    for {
      (txEndOffset, transactionId) <- submitAndReturnOffsetAndTransactionId(submitRequest)
      tx <- transactionClient
        .getTransactions(
          txEndOffset,
          None,
          transactionFilter
        )
        .filter(_.transactionId == transactionId)
        .take(1)
        .takeWithin(scaled(5.seconds))
        .runWith(Sink.headOption)
    } yield {
      withClue(
        s"No transactions are expected to be received as result of command ${submitRequest.commands.map(_.commandId).getOrElse("(empty id)")}") {
        tx shouldBe empty
      }
    }
  }

  /** Submit the command, then listen as the submitter for the transactionID, such that test code can run assertions
    * on the presence and contents of that transaction as other parties.
    */
  def submitAndReturnOffsetAndTransactionId(
      submitRequest: SubmitRequest): Future[(LedgerOffset, String)] =
    for {
      offsetBeforeSubmission <- timeout(
        submitSuccessfullyAndReturnOffset(submitRequest),
        s"Submitting command ${submitRequest.getCommands.commandId} as party ${submitRequest.getCommands.party}"
      )(mat.system)
      transactionId <- timeout(
        transactionClient
          .getTransactions(
            offsetBeforeSubmission,
            None,
            TransactionFilter(Map(submitRequest.getCommands.party -> Filters.defaultInstance)))
          .filter(_.commandId == submitRequest.getCommands.commandId)
          .map(_.transactionId)
          .runWith(Sink.head),
        s"Reading result of command ${submitRequest.getCommands.commandId} as submitter ${submitRequest.getCommands.commandId}"
      )(mat.system)
    } yield offsetBeforeSubmission -> transactionId

  def submitAndListenForSingleTreeResultOfCommand(
      command: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true,
      verbose: Boolean = false,
      opDescription: String = ""): Future[TransactionTree] = {
    submitAndListenForAllResultsOfCommand(command, transactionFilter, filterCid, verbose).map {
      transactions =>
        {
          withClue(
            s"Transaction tree received in response to command ${command.commands.map(_.commandId).getOrElse("(empty id)")} " +
              s"on behalf of ${command.commands.map(_.party).getOrElse("")} " +
              s"${if (opDescription != "") s"as part of $opDescription "}" +
              s"should have length of 1") {
            transactions._2 should have length (1)
            transactions._2.headOption.value
          }
        }
    }
  }

  def submitAndListenForResultsOfCommand(
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true,
      verbose: Boolean = false): Future[immutable.Seq[Transaction]] = {
    val commandId = submitRequest.getCommands.commandId
    for {
      txEndOffset <- timeout(
        submitSuccessfullyAndReturnOffset(submitRequest),
        s"Submitting command ${submitRequest.getCommands.commandId} as party ${submitRequest.getCommands.party}"
      )(mat.system)
      transactions <- timeout(
        listenForResultOfCommand(
          transactionFilter,
          if (filterCid) Some(commandId) else None,
          txEndOffset,
          verbose),
        s"Reading result of command ${submitRequest.getCommands.commandId} as submitter ${submitRequest.getCommands.commandId}"
      )(mat.system)
    } yield {
      transactions
    }
  }

  def submitAndListenForAllResultsOfCommand(
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true,
      verbose: Boolean = false)
    : Future[(immutable.Seq[Transaction], immutable.Seq[TransactionTree])] = {
    val commandId = submitRequest.getCommands.commandId
    for {
      txEndOffset <- submitSuccessfullyAndReturnOffset(submitRequest)
      transactionTrees <- timeout(
        listenForTreeResultOfCommand(
          transactionFilter,
          if (filterCid) Some(commandId) else None,
          txEndOffset,
          verbose),
        s"Reading tree result of command ${submitRequest.getCommands.commandId} as submitter ${submitRequest.getCommands.commandId}"
      )(mat.system)
      transactions <- timeout(
        listenForResultOfCommand(
          transactionFilter,
          if (filterCid) Some(commandId) else None,
          txEndOffset,
          verbose),
        s"Reading result of command ${submitRequest.getCommands.commandId} as submitter ${submitRequest.getCommands.commandId}"
      )(mat.system)
    } yield {
      (transactions, transactionTrees)
    }
  }

  def listenForResultOfCommand(
      transactionFilter: TransactionFilter,
      commandId: Option[String],
      txEndOffset: LedgerOffset,
      verbose: Boolean = false): Future[immutable.Seq[Transaction]] = {

    transactionClient
      .getTransactions(
        txEndOffset,
        None,
        transactionFilter,
        verbose
      )
      .filter(x => commandId.fold(true)(cid => x.commandId == cid))
      .take(1)
      .takeWithin(scaled(15.seconds))
      .runWith(Sink.seq)
  }

  /**
    * This is meant to be ran from [timeout] block, as it has no timeout mechanism of its own.
    */
  def listenForTreeResultOfCommand(
      transactionFilter: TransactionFilter,
      commandId: Option[String],
      txEndOffset: LedgerOffset,
      verbose: Boolean = false): Future[immutable.Seq[TransactionTree]] = {
    transactionClient
      .getTransactionTrees(
        txEndOffset,
        None,
        transactionFilter,
        verbose
      )
      .filter(x => commandId.fold(true)(cid => x.commandId == cid))
      .take(1)
      .runWith(Sink.seq)
  }

  def createdEventsIn(transaction: Transaction): Seq[CreatedEvent] =
    transaction.events.map(_.event).collect {
      case Created(createdEvent) =>
        createdEvent
    }

  //TODO this class contains a lot of duplicate code.
  def topLevelExercisedIn(transaction: TransactionTree): Seq[ExercisedEvent] =
    exercisedEventsInNodes(transaction.rootEventIds.map(evId => transaction.eventsById(evId)))

  def createdEventsInNodes(nodes: Seq[Event]): Seq[CreatedEvent] =
    nodes.map(_.event).collect {
      case Created(createdEvent) => createdEvent
    }

  def createdEventsInTreeNodes(nodes: Seq[TreeEvent]): Seq[CreatedEvent] =
    nodes.map(_.kind).collect {
      case TreeEvent.Kind.Created(createdEvent) => createdEvent
    }

  def exercisedEventsInNodes(nodes: Iterable[TreeEvent]): Seq[ExercisedEvent] =
    nodes
      .map(_.kind)
      .collect {
        case TreeEvent.Kind.Exercised(exercisedEvent) => exercisedEvent
      }(breakOut)

  def archivedEventsIn(transaction: Transaction): Seq[ArchivedEvent] =
    transaction.events.map(_.event).collect {
      case Archived(archivedEvent) => archivedEvent
    }

  /**
    * @return A LedgerOffset before the result transaction. If the ledger under test is used concurrently, the returned
    *         offset can be arbitrary distanced from the submitted command.
    */
  def submitSuccessfullyAndReturnOffset(submitRequest: SubmitRequest): Future[LedgerOffset] = {
    for {
      txEndOffset <- transactionClient.getLedgerEnd.map(_.getOffset)
      _ <- submitSuccessfully(submitRequest)
    } yield txEndOffset
  }

  def findCreatedEventIn(
      contractCreationTx: Transaction,
      templateToLookFor: Identifier): CreatedEvent = {
    // for helpful scalatest error message
    createdEventsIn(contractCreationTx).flatMap(_.templateId.toList) should contain(
      templateToLookFor)
    createdEventsIn(contractCreationTx).find(_.templateId.contains(templateToLookFor)).value
  }

  def getHead[T](elements: Iterable[T]): T = {
    elements should have size 1
    elements.headOption.value
  }

  def getField(record: Record, fieldName: String): Value = {
    val presentFieldNames: Set[String] = record.fields.map(_.label)(breakOut)
    presentFieldNames should contain(fieldName) // This check is duplicate, but offers better scalatest error message

    getHead(record.fields.collect {
      case RecordField(`fieldName`, fieldValue) => fieldValue
    }).value
  }

  def recordWithArgument(original: Record, fieldToInclude: RecordField): Record = {
    original.update(_.fields.modify(recordFieldsWithArgument(_, fieldToInclude)))
  }
  def recordFieldsWithArgument(
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

  // Create a template instance and return the resulting create event.
  def simpleCreateWithListener(
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] = {
    for {
      tx <- submitAndListenForSingleResultOfCommand(
        submitRequestWithId(commandId, submitter)
          .update(_.commands.commands := List(CreateCommand(Some(template), Some(arg)).wrap)),
        TransactionFilter(Map(listener -> Filters.defaultInstance))
      )
    } yield {
      getHead(createdEventsIn(tx))
    }
  }

  // Create a template instance and return the resulting create event.
  def simpleCreate(
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] =
    simpleCreateWithListener(commandId, submitter, submitter, template, arg)

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

  // Exercise a choice and return all resulting create events.
  def simpleExerciseWithListener(
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[TransactionTree] = {
    submitAndListenForSingleTreeResultOfCommand(
      submitRequestWithId(commandId, submitter)
        .update(
          _.commands.commands :=
            List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap)
        ),
      TransactionFilter(Map(listener -> Filters.defaultInstance)),
      false
    )
  }

  // Exercise a choice by key and return all resulting create events.
  def simpleExerciseByKeyWithListener(
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractKey: Value,
      choice: String,
      arg: Value
  ): Future[TransactionTree] = {
    submitAndListenForSingleTreeResultOfCommand(
      submitRequestWithId(commandId, submitter)
        .update(
          _.commands.commands :=
            List(ExerciseByKeyCommand(Some(template), Some(contractKey), choice, Some(arg)).wrap)
        ),
      TransactionFilter(Map(listener -> Filters.defaultInstance)),
      false
    )
  }

  def simpleCreateWithListenerForTransactions(
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      arg: Record
  ): Future[CreatedEvent] = {
    for {
      tx <- submitAndListenForSingleResultOfCommand(
        submitRequestWithId(commandId, submitter)
          .update(
            _.commands.commands :=
              List(CreateCommand(Some(template), Some(arg)).wrap)
          ),
        TransactionFilter(Map(listener -> Filters.defaultInstance))
      )
    } yield {
      getHead(createdEventsIn(tx))
    }
  }

  def submitAndListenForTransactionResultOfCommand(
      command: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true): Future[Seq[Transaction]] = {
    submitAndListenForTransactionResultsOfCommand(command, transactionFilter, filterCid)
  }

  def submitAndListenForTransactionResultsOfCommand(
      submitRequest: SubmitRequest,
      transactionFilter: TransactionFilter,
      filterCid: Boolean = true): Future[immutable.Seq[Transaction]] = {
    val commandId = submitRequest.getCommands.commandId
    for {
      txEndOffset <- submitSuccessfullyAndReturnOffset(submitRequest)
      transactions <- listenForTransactionResultOfCommand(
        transactionFilter,
        if (filterCid) Some(commandId) else None,
        txEndOffset)
    } yield {
      transactions
    }
  }

  def listenForTransactionResultOfCommand(
      transactionFilter: TransactionFilter,
      commandId: Option[String],
      txEndOffset: LedgerOffset): Future[immutable.Seq[Transaction]] = {
    transactionClient
      .getTransactions(
        txEndOffset,
        None,
        transactionFilter
      )
      .filter(x => commandId.fold(true)(cid => x.commandId == cid))
      .take(1)
      .takeWithin(scaled(3.seconds))
      .runWith(Sink.seq)
  }

  def simpleExerciseWithListenerForTransactions(
      commandId: String,
      submitter: String,
      listener: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[Seq[Transaction]] = {
    submitAndListenForTransactionResultOfCommand(
      submitRequestWithId(commandId, submitter)
        .update(
          _.commands.commands :=
            List(ExerciseCommand(Some(template), contractId, choice, Some(arg)).wrap)
        ),
      TransactionFilter(Map(listener -> Filters.defaultInstance)),
      false
    )
  }

  def transactionsFromSimpleExercise(
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value): Future[Seq[Transaction]] =
    simpleExerciseWithListenerForTransactions(
      commandId,
      submitter,
      submitter,
      template,
      contractId,
      choice,
      arg)

  def assertCommandFailsWithCode(
      submitRequest: SubmitRequest,
      expectedErrorCode: Code,
      expectedMessageSubString: String): Future[Assertion] = {
    for {
      ledgerEnd <- transactionClient.getLedgerEnd
      completion <- submitCommand(submitRequest)
      // TODO(FM) in the contract keys test this hangs forever after expecting a failedExercise.
      // Could it be that the ACS behaves like that sometimes? In that case that'd be a bug. We must investigate
      /*
      txs <- listenForResultOfCommand(
        getAllContracts(List(submitRequest.getCommands.party)),
        Some(submitRequest.getCommands.commandId),
        ledgerEnd.getOffset)
     */
    } yield {
      completion.getStatus should have('code (expectedErrorCode.value))
      completion.getStatus.message should include(expectedMessageSubString)
      // txs shouldBe empty
    }
  }

  def submitRequestWithId(commandId: String, submitter: String): SubmitRequest =
    M.submitRequest.update(
      _.commands.party := submitter,
      _.commands.commandId := commandId,
      _.commands.ledgerId := context.ledgerId.unwrap)

  // Exercise a choice and return all resulting create events.
  def simpleExercise(
      commandId: String,
      submitter: String,
      template: Identifier,
      contractId: String,
      choice: String,
      arg: Value
  ): Future[TransactionTree] =
    simpleExerciseWithListener(commandId, submitter, submitter, template, contractId, choice, arg)

  def simpleExerciseByKey(
      commandId: String,
      submitter: String,
      template: Identifier,
      contractKey: Value,
      choice: String,
      arg: Value
  ): Future[TransactionTree] =
    simpleExerciseByKeyWithListener(
      commandId,
      submitter,
      submitter,
      template,
      contractKey,
      choice,
      arg)

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

  // Exercise a choice by key that is supposed to fail.
  def failingExerciseByKey(
      commandId: String,
      submitter: String,
      template: Identifier,
      contractKey: Value,
      choice: String,
      arg: Value,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      submitRequestWithId(commandId, submitter)
        .update(
          _.commands.commands :=
            List(ExerciseByKeyCommand(Some(template), contractKey, choice, Some(arg)).wrap)
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

object LedgerTestingHelpers extends OptionValues {

  def apply(
      submitCommand: SubmitAndWaitRequest => Future[SubmitAndWaitForTransactionIdResponse],
      context: LedgerContext,
      timeoutScaleFactor: Double = 1.0)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer): LedgerTestingHelpers =
    new LedgerTestingHelpers(helper(submitCommand), context, timeoutScaleFactor)

  def responseToCompletion(commandId: String, respF: Future[SubmitAndWaitForTransactionIdResponse])(
      implicit ec: ExecutionContext): Future[Completion] =
    respF
      .map(
        tx =>
          Completion(
            commandId,
            Some(Status(io.grpc.Status.OK.getCode.value(), "")),
            tx.transactionId))
      .recover {
        case sre: StatusRuntimeException =>
          Completion(
            commandId,
            Some(Status(sre.getStatus.getCode.value(), sre.getStatus.getDescription)))
      }

  private def helper(
      submitCommand: SubmitAndWaitRequest => Future[SubmitAndWaitForTransactionIdResponse])(
      implicit ec: ExecutionContext) = { req: SubmitRequest =>
    responseToCompletion(
      req.commands.value.commandId,
      submitCommand(SubmitAndWaitRequest(req.commands, req.traceContext)))
  }
}
