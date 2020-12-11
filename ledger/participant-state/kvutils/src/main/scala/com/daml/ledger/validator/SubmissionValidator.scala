// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.Timer
import com.daml.caching.{Cache, ConcurrentCache}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.{Bytes, DamlStateMap, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator._
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.github.ghik.silencer.silent
import com.google.protobuf.ByteString

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Orchestrates validating, transforming or committing submissions for key-value ledgers.
  *
  * @param ledgerStateAccess     defines how the validator retrieves/writes back state to the ledger
  * @param processSubmission     defines how a log entry and state updates are generated
  * @param allocateLogEntryId    defines how new log entry IDs are being generated
  * @param checkForMissingInputs whether all inputs declared as the required inputs in the
  *                              submission must be available in order to pass validation
  * @param stateValueCache        a cache for deserializing state values from bytes
  * @param metrics                defines the metric names
  */
class SubmissionValidator[LogResult] private[validator] (
    ledgerStateAccess: LedgerStateAccess[LogResult],
    processSubmission: SubmissionValidator.ProcessSubmission,
    allocateLogEntryId: () => DamlLogEntryId,
    checkForMissingInputs: Boolean,
    stateValueCache: ConcurrentCache[Bytes, DamlStateValue],
    metrics: Metrics,
) {

  private val logger = ContextualizedLogger.get(getClass)

  private val timedLedgerStateAccess = new TimedLedgerStateAccess(ledgerStateAccess)

  def validate(
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
  )(implicit executionContext: ExecutionContext): Future[Either[ValidationFailed, Unit]] =
    newLoggingContext { implicit loggingContext =>
      runValidation(
        envelope,
        correlationId,
        recordTime,
        participantId,
        postProcessResult = (_, _, _, _) => Future.unit,
        postProcessResultTimer = None,
      )
    }

  def validateAndCommit(
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
  )(implicit executionContext: ExecutionContext): Future[Either[ValidationFailed, LogResult]] =
    newLoggingContext { implicit loggingContext =>
      validateAndCommitWithContext(envelope, correlationId, recordTime, participantId)
    }

  private[validator] def validateAndCommitWithContext(
      envelope: Bytes,
      correlationId: String,
      recordTime: Timestamp,
      participantId: ParticipantId,
  )(
      implicit executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ValidationFailed, LogResult]] =
    runValidation(
      envelope,
      correlationId,
      recordTime,
      participantId,
      commit,
      Some(metrics.daml.kvutils.submission.validator.commit),
    )

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
  )(implicit executionContext: ExecutionContext): Future[Either[ValidationFailed, U]] =
    newLoggingContext { implicit loggingContext =>
      runValidation(
        envelope,
        correlationId,
        recordTime,
        participantId,
        transform,
        Some(metrics.daml.kvutils.submission.validator.transformSubmission),
      )
    }

  @silent(" ignored .* is never used") // matches runValidation signature
  private def commit(
      logEntryId: DamlLogEntryId,
      ignored: Any,
      logEntryAndState: LogEntryAndState,
      stateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[LogResult] = {
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

  @tailrec
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
      postProcessResultTimer: Option[Timer],
  )(
      implicit executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ValidationFailed, T]] =
    metrics.daml.kvutils.submission.validator.openEnvelope
      .time(() => Envelope.open(envelope)) match {
      case Right(Envelope.SubmissionBatchMessage(batch)) =>
        // NOTE(JM)): We support validation of batches of size 1, but not more as batch validation
        // does not currently fit these interfaces (e.g. multiple "LogResult"s).
        // A separate batch validator is used instead for the time being, but we support batches here
        // to allow testing and integration of BatchingLedgerWriter.
        batch.getSubmissionsList.asScala.toList match {
          case correlatedSubmission :: Nil =>
            runValidation(
              correlatedSubmission.getSubmission,
              correlatedSubmission.getCorrelationId,
              recordTime,
              participantId,
              postProcessResult,
              postProcessResultTimer)
          case submissions =>
            logger.error(s"Unsupported batch size of ${submissions.length}, rejecting submission.")
            Future.successful(Left(
              ValidationFailed.ValidationError(s"Unsupported batch size of ${submissions.length}")))
        }

      case Right(Envelope.SubmissionMessage(submission)) =>
        val damlLogEntryId = allocateLogEntryId()
        val declaredInputs = submission.getInputDamlStateList.asScala
        val inputKeysAsBytes = declaredInputs.map(keyToBytes)
        timedLedgerStateAccess
          .inTransaction { stateOperations =>
            for {
              readInputs <- Timed.future(
                metrics.daml.kvutils.submission.validator.fetchInputs,
                for {
                  readStateValues <- stateOperations.readState(inputKeysAsBytes)
                  readInputs = readStateValues.view
                    .zip(declaredInputs)
                    .map {
                      case (valueBytes, key) =>
                        (key, valueBytes.map(stateValueCache.getOrAcquire(_, bytesToStateValue)))
                    }
                    .toMap
                  _ <- verifyAllInputsArePresent(declaredInputs, readInputs)
                } yield readInputs
              )
              logEntryAndState <- Timed.future(
                metrics.daml.kvutils.submission.validator.validate,
                Future.fromTry(
                  Try(
                    processSubmission(
                      damlLogEntryId,
                      recordTime,
                      submission,
                      participantId,
                      readInputs)))
              )
              processResult = () =>
                postProcessResult(
                  damlLogEntryId,
                  flattenInputStates(readInputs),
                  logEntryAndState,
                  stateOperations,
              )
              result <- postProcessResultTimer.fold(processResult())(
                Timed.future(_, processResult()))
            } yield result
          }
          .transform {
            case Success(result) =>
              Success(Right(result))
            case Failure(exception: ValidationFailed) =>
              Success(Left(exception))
            case Failure(exception) =>
              logger.error("Unexpected failure during submission validation.", exception)
              Success(Left(ValidationError(exception.getLocalizedMessage)))
          }
      case _ =>
        Future.successful(
          Left(ValidationError(s"Failed to parse submission, correlationId=$correlationId")))
    }

  private def verifyAllInputsArePresent[T](
      declaredInputs: Seq[DamlStateKey],
      readInputs: DamlStateMap,
  ): Future[Unit] = {
    if (checkForMissingInputs) {
      val missingInputs = declaredInputs.toSet -- readInputs.filter(_._2.isDefined).keySet
      if (missingInputs.nonEmpty) {
        Future.failed(MissingInputState(missingInputs.map(keyToBytes).toSeq))
      } else {
        Future.unit
      }
    } else {
      Future.unit
    }
  }

  private def flattenInputStates(
      inputs: DamlStateMap
  ): Map[DamlStateKey, DamlStateValue] =
    inputs.collect {
      case (key, Some(value)) => key -> value
    }

  private final class TimedLedgerStateAccess(delegate: LedgerStateAccess[LogResult])
      extends LedgerStateAccess[LogResult] {
    override def inTransaction[T](
        body: LedgerStateOperations[LogResult] => Future[T]
    )(implicit executionContext: ExecutionContext): Future[T] = {
      // This is necessary to ensure we capture successful and failed acquisitions separately.
      // These need to be measured separately as they may have very different characteristics.
      val acquisitionWasRecorded = new AtomicBoolean(false)
      val successfulAcquisitionTimer =
        metrics.daml.kvutils.submission.validator.acquireTransactionLock.time()
      val failedAcquisitionTimer =
        metrics.daml.kvutils.submission.validator.failedToAcquireTransaction.time()
      delegate
        .inTransaction { operations =>
          if (acquisitionWasRecorded.compareAndSet(false, true)) {
            successfulAcquisitionTimer.stop()
          }
          body(operations)
            .transform(result =>
              Success(
                (result, metrics.daml.kvutils.submission.validator.releaseTransactionLock.time())))
        }
        .transform {
          case Success((result, releaseTimer)) =>
            releaseTimer.stop()
            result
          case Failure(exception) =>
            if (acquisitionWasRecorded.compareAndSet(false, true)) {
              failedAcquisitionTimer.stop()
            }
            Failure(exception)
        }
    }
  }
}

object SubmissionValidator {

  type RawKeyValuePairs = Seq[(Bytes, Bytes)]

  type StateMap = Map[DamlStateKey, DamlStateValue]
  type LogEntryAndState = (DamlLogEntry, StateMap)

  private[validator] type RecordTime = Timestamp
  private[validator] type InputState = DamlStateMap

  private[validator] type ProcessSubmission = (
      DamlLogEntryId,
      RecordTime,
      DamlSubmission,
      ParticipantId,
      InputState,
  ) => LogEntryAndState

  def create[LogResult](
      ledgerStateAccess: LedgerStateAccess[LogResult],
      allocateNextLogEntryId: () => DamlLogEntryId = () => allocateRandomLogEntryId(),
      checkForMissingInputs: Boolean = false,
      stateValueCache: ConcurrentCache[Bytes, DamlStateValue] = Cache.none,
      engine: Engine,
      metrics: Metrics,
  ): SubmissionValidator[LogResult] = {
    createForTimeMode(
      ledgerStateAccess,
      allocateNextLogEntryId,
      checkForMissingInputs,
      stateValueCache,
      engine,
      metrics,
      inStaticTimeMode = false,
    )
  }

  // Internal method to enable proper command dedup in sandbox with static time mode
  private[daml] def createForTimeMode[LogResult](
      ledgerStateAccess: LedgerStateAccess[LogResult],
      allocateNextLogEntryId: () => DamlLogEntryId = () => allocateRandomLogEntryId(),
      checkForMissingInputs: Boolean = false,
      stateValueCache: ConcurrentCache[Bytes, DamlStateValue] = Cache.none,
      engine: Engine,
      metrics: Metrics,
      inStaticTimeMode: Boolean,
  ): SubmissionValidator[LogResult] =
    new SubmissionValidator(
      ledgerStateAccess,
      processSubmission(new KeyValueCommitting(engine, metrics, inStaticTimeMode)),
      allocateNextLogEntryId,
      checkForMissingInputs,
      stateValueCache,
      metrics,
    )

  private[validator] def allocateRandomLogEntryId(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()

  private[validator] def processSubmission(keyValueCommitting: KeyValueCommitting)(
      damlLogEntryId: DamlLogEntryId,
      recordTime: Timestamp,
      damlSubmission: DamlSubmission,
      participantId: ParticipantId,
      inputState: DamlStateMap,
  ): LogEntryAndState =
    keyValueCommitting.processSubmission(
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
      damlStateUpdates
        .map {
          case (key, value) => keyToBytes(key) -> valueToBytes(value)
        }
        .toSeq
        .sortBy(_._1.asReadOnlyByteBuffer())
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
