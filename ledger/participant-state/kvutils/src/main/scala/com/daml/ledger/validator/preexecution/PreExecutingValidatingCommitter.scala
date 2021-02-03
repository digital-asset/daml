// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.export.{
  LedgerDataExporter,
  SubmissionAggregatorWriteOperations,
  SubmissionInfo,
}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.reading.{LedgerStateReader, StateReader}
import com.daml.ledger.validator.{
  CombinedLedgerStateWriteOperations,
  LedgerStateAccess,
  LedgerStateOperationsReaderAdapter,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** A pre-executing validating committer based on [[LedgerStateAccess]] (that does not provide
  * fingerprints alongside values), parametric in the logic that produces a fingerprint given a
  * value.
  *
  * @param now                           Returns the current time.
  * @param transformStateReader          Transforms the state reader into the format used by the underlying store.
  * @param validator                     The pre-execution validator.
  * @param postExecutionConflictDetector The post-execution conflict detector.
  * @param postExecutionWriter           The post-execution writer.
  * @param ledgerDataExporter            Exports to a file.
  */
class PreExecutingValidatingCommitter[StateValue, ReadSet, WriteSet](
    now: () => Instant,
    transformStateReader: LedgerStateReader => StateReader[DamlStateKey, StateValue],
    validator: PreExecutingSubmissionValidator[StateValue, ReadSet, WriteSet],
    postExecutionConflictDetector: PostExecutionConflictDetector[
      DamlStateKey,
      StateValue,
      ReadSet,
      WriteSet,
    ],
    postExecutionWriter: PostExecutionWriter[WriteSet],
    ledgerDataExporter: LedgerDataExporter,
) {

  private val logger = ContextualizedLogger.get(getClass)

  /** Pre-executes and then commits a submission.
    */
  def commit(
      submittingParticipantId: ParticipantId,
      correlationId: String,
      submissionEnvelope: Raw.Value,
      recordTime: Instant,
      ledgerStateAccess: LedgerStateAccess[Any],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    LoggingContext.newLoggingContext("correlationId" -> correlationId) { implicit loggingContext =>
      val submissionInfo =
        SubmissionInfo(submittingParticipantId, correlationId, submissionEnvelope, recordTime)
      val submissionAggregator = ledgerDataExporter.addSubmission(submissionInfo)
      // Sequential pre-execution, implemented by enclosing the whole pre-post-exec pipeline is a single transaction.
      ledgerStateAccess.inTransaction { ledgerStateOperations =>
        val stateReader =
          transformStateReader(new LedgerStateOperationsReaderAdapter(ledgerStateOperations))
        for {
          preExecutionOutput <- validator.validate(
            submittingParticipantId,
            submissionEnvelope,
            recordTime,
            stateReader,
          )
          _ <- retry { case _: ConflictDetectedException =>
            logger.error("Conflict detected during post-execution. Retrying...")
            true
          } { (_, _) =>
            postExecutionConflictDetector.detectConflicts(preExecutionOutput, stateReader)
          }.transform {
            case Failure(_: ConflictDetectedException) =>
              logger.error("Too many conflicts detected during post-execution. Giving up.")
              Success(SubmissionResult.Acknowledged) // But it will simply be dropped.
            case result => result
          }
          writeSet = selectWriteSet(preExecutionOutput)
          submissionResult <- postExecutionWriter.write(
            writeSet,
            new CombinedLedgerStateWriteOperations(
              ledgerStateOperations,
              new SubmissionAggregatorWriteOperations(submissionAggregator.addChild()),
              (_: Any, _: Unit) => (),
            ),
          )
        } yield {
          submissionAggregator.finish()
          submissionResult
        }
      }
    }

  private def selectWriteSet(
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet]
  ): WriteSet = {
    val recordTime = now()
    val withinTimeBounds =
      !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
        !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))
    if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
    } else {
      preExecutionOutput.outOfTimeBoundsWriteSet
    }
  }

  private[this] def retry: PartialFunction[Throwable, Boolean] => RetryStrategy =
    RetryStrategy.constant(attempts = Some(3), 5.seconds)

}
