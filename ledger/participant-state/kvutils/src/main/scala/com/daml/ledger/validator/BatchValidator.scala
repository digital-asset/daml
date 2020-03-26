// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.{ConflictDetection, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator.LogEntryAndState
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.logging.ContextualizedLogger
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Parameters for batch validation.
  *
  * @param cpuParallelism Amount of parallelism to use for CPU bound steps.
  * @param ioParallelism Amount of parallelism to use for ledger state operations.
  */
case class BatchValidationParameters(
    cpuParallelism: Int,
    ioParallelism: Int
)

object BatchValidationParameters {
  // FIXME figure out good defaults. And what the granularity for parallelism should be. Does the
  // CPU and IO split make sense?
  def default: BatchValidationParameters = BatchValidationParameters(8, 8)
}

object BatchValidator {

  def apply(
      allocateEntryId: () => DamlLogEntryId,
      ledgerState: BatchLedgerState,
      params: BatchValidationParameters = BatchValidationParameters.default): BatchValidator =
    newRawBatchValidator(
      allocateEntryId,
      RawBatchLedgerState.adaptBatchLedgerState(ledgerState),
      params
    )

  // This is the internal API we'd use when transformations of log/state are required.
  def newRawBatchValidator(
      allocateEntryId: () => DamlLogEntryId,
      rawLedgerState: RawBatchLedgerState,
      params: BatchValidationParameters = BatchValidationParameters.default): BatchValidator =
    new BatchValidator(
      params,
      Engine(),
      allocateEntryId,
      rawLedgerState
    )
}

class BatchValidator private (
    val params: BatchValidationParameters,
    val engine: Engine,
    val allocateLogEntryId: () => DamlLogEntryId,
    val ledgerState: RawBatchLedgerState) {

  private val logger = ContextualizedLogger.get(getClass)

  // TODO(JM): How to report errors? Future[Unit] that fails or Future[Either[ValidationError, Unit]] that never
  // fails? We're expecting this to fail only when a batch is corrupted or malicious (and out of memory etc.).
  def validateAndCommit(
      recordTimeInstant: Instant,
      participantId: ParticipantId,
      correlationId: String,
      envelope: ByteString)(implicit materializer: Materializer): Future[Unit] = {

    val recordTime = Time.Timestamp.assertFromInstant(recordTimeInstant)
    implicit val executionContext: ExecutionContext = materializer.executionContext

    newLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      Envelope.open(envelope) match {
        case Right(Envelope.BatchMessage(batch)) =>
          // TODO(JM): Here we're doing everything in one transaction. In principle if the assumption holds
          // that there's a single validator that sequentially processes the submissions, then we can do the
          // reads and writes in separate transactions as we can assume there's no other writers. This assumption
          // would break with DAML-on-SQL with multiple writers.
          logger.trace(s"Validating batch of ${batch.getSubmissionsCount} submissions")
          ledgerState.inTransaction { stateOps =>
            Source
              .fromIterator(() => batch.getSubmissionsList.asScala.toIterator)

              // Uncompress the submissions in parallel.
              .mapAsyncUnordered(params.cpuParallelism) { cs =>
                Future {
                  // FIXME(JM: metrics
                  //println("UNCOMPRESS")
                  cs.getCorrelationId -> Envelope
                    .openSubmission(cs.getSubmission)
                    .fold(err => throw ValidationFailed.ValidationError(err), identity)
                }
              }

              // Fetch the submission inputs in parallel.
              // NOTE(JM): We assume the underlying ledger state access caches reads within transaction
              // and hence make no effort here to deduplicate reads.
              // FIXME(JM): Since we're wrapped within "inTransaction" doing these in parallel may or may not
              // help. See comment on top about using separate ledger state transactions.
              .mapAsyncUnordered(params.ioParallelism) {
                case (correlationId, submission) =>
                  // FIXME(JM: metrics
                  val inputKeys = submission.getInputDamlStateList.asScala
                  stateOps
                    .readState(inputKeys)
                    .map { values =>
                      //println("READ STATE")
                      (
                        correlationId,
                        submission,
                        inputKeys.zip(values).toMap
                      )
                    }
              }

              // Validate the submissions in parallel.
              .mapAsyncUnordered(params.cpuParallelism) {
                case (submissionCorrelationId, submission, inputState) =>
                  Future {
                    val damlLogEntryId = allocateLogEntryId()
                    //println("VALIDATE")
                    // FIXME(JM: metrics

                    (
                      correlationId,
                      damlLogEntryId,
                      processSubmission(
                        damlLogEntryId,
                        recordTime,
                        submission,
                        participantId,
                        inputState),
                      inputState)

                  }
              }

              // Sequentially detect conflicts and choose the appropriate result, or drop the
              // submission if the conflicts cannot be resolved.
              .statefulMapConcat { () =>
                // The set of keys that have been written to. Submissions that have used these as
                // inputs are considering conflicted.
                val invalidatedKeys = mutable.Set.empty[DamlStateKey]

                {
                  case (submissionCorrelationId, logEntryId, (logEntry, outputState), inputState) =>
                    newLoggingContext("correlationId" -> submissionCorrelationId) {
                      implicit logCtx =>
                        ConflictDetection.conflictDetectAndRecover(
                          invalidatedKeys,
                          inputState.keySet,
                          logEntry,
                          outputState
                        ) match {
                          case Some((newLogEntry, newState)) =>
                            invalidatedKeys ++= newState.keySet
                            (logEntryId, (newLogEntry, inputState, newState)) :: Nil

                          case None =>
                            logger.info(
                              s"Submission dropped as it conflicted and recovery was not possible")
                            Nil
                        }
                    }
                }
              }

              // Serialize and commit the results in parallel.
              .mapAsyncUnordered(params.ioParallelism) {
                case (logEntryId, (logEntry, inputState, outputState)) =>
                  //println("SERIALIZE AND WRITE")
                  // FIXME(JM: metrics
                  stateOps.commit(
                    logEntryId,
                    logEntry,
                    inputState,
                    outputState
                  )
              }
              .runWith(Sink.ignore)
              .map(_ => ())
          }

        case _ =>
          sys.error("Unsupported message type")
      }
    }
  }

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

}
