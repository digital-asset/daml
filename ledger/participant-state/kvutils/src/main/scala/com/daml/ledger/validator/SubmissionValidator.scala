// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator._
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Orchestrates validating, transforming or committing submissions for key-value ledgers.
  *
  * @param ledgerStateAccess defines how the validator retrieves/writes back state to the ledger
  * @param processSubmission defines how a log entry and state updates get generated from a submission
  * @param allocateLogEntryId  defines how new log entry IDs are being generated
  * @param checkForMissingInputs  whether all inputs declared as the required inputs in the submission must be available
  *                               in order to pass validation
  * @param executionContext  ExecutionContext to use when performing ledger state reads/writes
  */
class SubmissionValidator[LogResult](
    ledgerStateAccess: LedgerStateAccess[LogResult],
    processSubmission: (
        DamlLogEntryId,
        Timestamp,
        DamlSubmission,
        ParticipantId,
        Map[DamlStateKey, Option[DamlStateValue]],
    ) => LogEntryAndState,
    allocateLogEntryId: () => DamlLogEntryId,
    checkForMissingInputs: Boolean = false,
)(implicit executionContext: ExecutionContext) {

  private val logger = ContextualizedLogger.get(getClass)

  def validate(
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
  ): Future[Either[ValidationFailed, Unit]] =
    newLoggingContext { implicit logCtx =>
      runValidation(envelope, correlationId, recordTime, participantId, (_, _, _, _) => Future.unit)
    }

  def validateAndCommit(
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
  ): Future[Either[ValidationFailed, LogResult]] =
    newLoggingContext { implicit logCtx =>
      validateAndCommitWithLoggingContext(envelope, correlationId, recordTime, participantId)
    }

  private[validator] def validateAndCommitWithLoggingContext(
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
  )(implicit logCtx: LoggingContext): Future[Either[ValidationFailed, LogResult]] =
    runValidation(envelope, correlationId, recordTime, participantId, commit)

  def validateAndTransform[U](
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
      transform: (
          DamlLogEntryId,
          StateMap,
          LogEntryAndState,
          LedgerStateOperations[LogResult]) => Future[U]
  ): Future[Either[ValidationFailed, U]] =
    newLoggingContext { implicit logCtx =>
      runValidation(envelope, correlationId, recordTime, participantId, transform)
    }

  private def commit(
      logEntryId: DamlLogEntryId,
      ignored: StateMap,
      logEntryAndState: LogEntryAndState,
      stateOperations: LedgerStateOperations[LogResult],
  ): Future[LogResult] = {
    val (rawLogEntry, rawStateUpdates) = serializeProcessedSubmission(logEntryAndState)
    val eventualLogResult = stateOperations.appendToLog(logEntryId.toByteString, rawLogEntry)
    val eventualStateResult =
      if (rawStateUpdates.nonEmpty)
        stateOperations.writeState(rawStateUpdates)
      else
        Future.unit
    for {
      logResult <- eventualLogResult
      _ <- eventualStateResult
    } yield logResult
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def runValidation[T](
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
      postProcessResult: (
          DamlLogEntryId,
          StateMap,
          LogEntryAndState,
          LedgerStateOperations[LogResult],
      ) => Future[T],
  )(implicit logCtx: LoggingContext): Future[Either[ValidationFailed, T]] =
    Envelope.open(envelope) match {
      case Right(Envelope.SubmissionMessage(submission)) =>
        val declaredInputs = submission.getInputDamlStateList.asScala
        val inputKeysAsBytes = declaredInputs.map(keyToBytes)
        ledgerStateAccess.inTransaction { stateOperations =>
          val result = for {
            readStateValues <- stateOperations.readState(inputKeysAsBytes)
            readStateInputs = readStateValues.zip(declaredInputs).map {
              case (valueBytes, key) => (key, valueBytes.map(bytesToStateValue))
            }
            damlLogEntryId = allocateLogEntryId()
            readInputs: Map[DamlStateKey, Option[DamlStateValue]] = readStateInputs.toMap
            missingInputs = declaredInputs.toSet -- readInputs.filter(_._2.isDefined).keySet
            _ <- if (checkForMissingInputs && missingInputs.nonEmpty)
              Future.failed(MissingInputState(missingInputs.map(keyToBytes).toSeq))
            else
              Future.unit
            logEntryAndState <- Future.fromTry(Try(
              processSubmission(damlLogEntryId, recordTime, submission, participantId, readInputs)))
            result <- postProcessResult(
              damlLogEntryId,
              flattenInputStates(readInputs),
              logEntryAndState,
              stateOperations,
            )
          } yield result
          result.transform {
            case Success(result) =>
              Success(Right(result))
            case Failure(exception: ValidationFailed) =>
              Success(Left(exception))
            case Failure(exception) =>
              logger.error("Unexpected failure during submission validation.", exception)
              Success(Left(ValidationError(exception.getLocalizedMessage)))
          }
        }
      case _ =>
        Future.successful(
          Left(ValidationError(s"Failed to parse submission, correlationId=$correlationId")))
    }

  private def flattenInputStates(
      inputs: Map[DamlStateKey, Option[DamlStateValue]]): Map[DamlStateKey, DamlStateValue] =
    inputs.collect {
      case (key, Some(value)) => key -> value
    }
}

object SubmissionValidator {
  type RawKeyValuePairs = Seq[(Bytes, Bytes)]

  type StateMap = Map[DamlStateKey, DamlStateValue]
  type LogEntryAndState = (DamlLogEntry, StateMap)

  private lazy val engine = Engine()

  def create[LogResult](
      ledgerStateAccess: LedgerStateAccess[LogResult],
      allocateNextLogEntryId: () => DamlLogEntryId = () => allocateRandomLogEntryId(),
      checkForMissingInputs: Boolean = false,
  )(implicit executionContext: ExecutionContext): SubmissionValidator[LogResult] = {
    new SubmissionValidator(
      ledgerStateAccess,
      processSubmission,
      allocateNextLogEntryId,
      checkForMissingInputs,
    )
  }

  private[validator] def allocateRandomLogEntryId(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()

  private[validator] def processSubmission(
      damlLogEntryId: DamlLogEntryId,
      recordTime: Timestamp,
      damlSubmission: DamlSubmission,
      participantId: ParticipantId,
      inputState: Map[DamlStateKey, Option[DamlStateValue]]): LogEntryAndState =
    KeyValueCommitting.processSubmission(
      engine,
      damlLogEntryId,
      recordTime,
      LedgerReader.DefaultConfiguration,
      damlSubmission,
      participantId,
      inputState
    )

  private[validator] def serializeProcessedSubmission(
      logEntryAndState: LogEntryAndState): (Bytes, RawKeyValuePairs) = {
    val (logEntry, damlStateUpdates) = logEntryAndState
    val rawStateUpdates =
      damlStateUpdates.map {
        case (key, value) => keyToBytes(key) -> valueToBytes(value)
      }.toSeq
    (Envelope.enclose(logEntry), rawStateUpdates)
  }

  private[validator] def keyToBytes(damlStateKey: DamlStateKey): Bytes =
    damlStateKey.toByteString

  private[validator] def valueToBytes(value: DamlStateValue): Bytes =
    Envelope.enclose(value)

  private[validator] def bytesToStateValue(value: Bytes): DamlStateValue =
    Envelope
      .openStateValue(value)
      .fold(message => throw new IllegalStateException(message), identity)
}
