// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator.LogEntryAndState
import com.daml.ledger.validator.ValidationResult.{
  MissingInputState,
  SubmissionValidated,
  TransformedSubmission,
  ValidationError,
  ValidationFailed,
  ValidationResult
}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Orchestrates validating, transforming or committing submissions for key-value ledgers.
  *
  * @param ledgerStateAccess defines how the validator retrieves/writes back state to the ledger
  * @param processSubmission defines how a log entry and state updates get generated from a submission
  * @param allocateLogEntryId  defines how new log entry IDs are being generated
  * @param executionContext  ExecutionContext to use when performing ledger state reads/writes
  */
class SubmissionValidator(
    ledgerStateAccess: LedgerStateAccess,
    processSubmission: (
        DamlLogEntryId,
        Timestamp,
        DamlSubmission,
        Map[DamlStateKey, Option[DamlStateValue]]) => LogEntryAndState,
    allocateLogEntryId: () => DamlLogEntryId,
    checkForMissingInputs: Boolean = false)(implicit executionContext: ExecutionContext) {

  import SubmissionValidator._

  def validate(
      envelope: RawBytes,
      correlationId: String,
      recordTime: Timestamp): Future[ValidationResult] =
    runValidation(envelope, correlationId, recordTime, (_, _, _, _) => ()).map {
      case Left(failure) => failure
      case Right(_) => SubmissionValidated
    }

  def validateAndCommit(
      envelope: RawBytes,
      correlationId: String,
      recordTime: Timestamp): Future[ValidationResult] =
    runValidation(envelope, correlationId, recordTime, commit).map {
      case Left(failure) => failure
      case Right(_) => SubmissionValidated
    }

  def validateAndTransform[T](
      envelope: RawBytes,
      correlationId: String,
      recordTime: Timestamp,
      transform: (DamlLogEntryId, StateMap, LogEntryAndState) => T)
    : Future[Either[ValidationFailed, TransformedSubmission[T]]] = {
    def applyTransformation(
        logEntryId: DamlLogEntryId,
        inputStates: StateMap,
        logEntryAndState: LogEntryAndState,
        stateOperations: LedgerStateOperations): T =
      transform(logEntryId, inputStates, logEntryAndState)

    runValidation(envelope, correlationId, recordTime, applyTransformation)
  }

  private def commit(
      logEntryId: DamlLogEntryId,
      ignored: StateMap,
      logEntryAndState: LogEntryAndState,
      stateOperations: LedgerStateOperations): Unit = {
    val (rawLogEntry, rawStateUpdates) = serializeProcessedSubmission(logEntryAndState)
    Future
      .sequence(
        Seq(
          stateOperations.appendToLog(logEntryId.toByteArray, rawLogEntry),
          if (rawStateUpdates.nonEmpty) {
            stateOperations.writeState(rawStateUpdates)
          } else {
            Future.unit
          }
        )
      )
      .foreach(_ => ())
  }

  private def runValidation[T](
      envelope: RawBytes,
      correlationId: String,
      recordTime: Timestamp,
      postProcessResult: (DamlLogEntryId, StateMap, LogEntryAndState, LedgerStateOperations) => T)
    : Future[Either[ValidationFailed, TransformedSubmission[T]]] =
    Envelope.open(envelope) match {
      case Right(Envelope.SubmissionMessage(submission)) =>
        val declaredInputs = submission.getInputDamlStateList.asScala
        ledgerStateAccess.inTransaction { stateOperations =>
          val inputKeysAsBytes = declaredInputs.map(keyToBytes)
          for {
            readStateValues <- stateOperations.readState(inputKeysAsBytes)
            readStateInputs = readStateValues.zip(declaredInputs).map {
              case (valueBytes, key) => (key, valueBytes.map(bytesToStateValue))
            }
            damlLogEntryId = allocateLogEntryId()
            readInputs: Map[DamlStateKey, Option[DamlStateValue]] = readStateInputs.toMap
            missingInputs = declaredInputs.toSet -- readInputs.filter(_._2.isDefined).keySet
          } yield {
            if (checkForMissingInputs && missingInputs.nonEmpty) {
              Left(MissingInputState(missingInputs.map(keyToBytes).toSeq))
            } else {
              Try {
                val (logEntry, damlStateUpdates) =
                  processSubmission(damlLogEntryId, recordTime, submission, readInputs)
                postProcessResult(
                  damlLogEntryId,
                  flattenInputStates(readInputs),
                  (logEntry, damlStateUpdates),
                  stateOperations)
              }.fold(
                exception => Left(ValidationError(exception.getLocalizedMessage)),
                result => Right(TransformedSubmission(result)))
            }
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
  type RawBytes = Array[Byte]
  type RawKeyValuePairs = Seq[(RawBytes, RawBytes)]

  type StateMap = Map[DamlStateKey, DamlStateValue]
  type LogEntryAndState = (DamlLogEntry, StateMap)

  def create(
      ledgerStateAccess: LedgerStateAccess,
      allocateNextLogEntryId: () => DamlLogEntryId = () => allocateRandomLogEntryId(),
      checkForMissingInputs: Boolean = false)(
      implicit executionContext: ExecutionContext): SubmissionValidator = {
    val participantId: ParticipantId =
      ParticipantId.assertFromString(ledgerStateAccess.participantId)
    new SubmissionValidator(
      ledgerStateAccess,
      processSubmission(participantId),
      allocateNextLogEntryId,
      checkForMissingInputs)
  }

  def allocateRandomLogEntryId(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()

  private lazy val engine = Engine()

  private[validator] def processSubmission(participantId: ParticipantId)(
      damlLogEntryId: DamlLogEntryId,
      recordTime: Timestamp,
      damlSubmission: DamlSubmission,
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
      logEntryAndState: LogEntryAndState): (RawBytes, RawKeyValuePairs) = {
    val (logEntry, damlStateUpdates) = logEntryAndState
    val rawStateUpdates = damlStateUpdates
      .map {
        case (key, value) => keyToBytes(key) -> valueToBytes(value)
      }
      .toSeq
      .sortBy(_._1.toIterable)
    (Envelope.enclose(logEntry).toByteArray, rawStateUpdates)
  }

  private[validator] def keyToBytes(damlStateKey: DamlStateKey): RawBytes =
    KeyValueCommitting.packDamlStateKey(damlStateKey).toByteArray

  private[validator] def valueToBytes(value: DamlStateValue): RawBytes =
    Envelope
      .enclose(value)
      .toByteArray

  private[validator] def bytesToStateValue(value: RawBytes): DamlStateValue =
    Envelope.openStateValue(value).right.get
}
