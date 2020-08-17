// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.{Bytes, Fingerprint}
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.LedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.Value

import scala.concurrent.{ExecutionContext, Future}

/**
  * An in-transaction post-execution finalizer to be invoked as the last stage of a pre-execution pipeline that
  * performs deferred time bound checks, detects pre-execution conflicts and persists both state changes and
  * a suitable log entry.
  *
  * It access the ledger state through [[LedgerStateOperations]] that doesn't provide fingerprints along values
  * and so it is parametric in the logic that produces a fingerprint given a value.
  *
  * @param valueToFingerprint The logic producing a [[Fingerprint]] given a value.
  * @tparam LogResult Type of the offset used for a log entry.
  */
class PostExecutionFinalizer[LogResult](valueToFingerprint: Option[Value] => Fingerprint) {

  import PostExecutionFinalizer._

  /**
    * @param now The logic that produces the current instant for record time bounds check and record time finality.
    * @param preExecutionOutput The output from the pre-execution stage.
    * @param ledgerStateOperations The operations that can access actual ledger storage as part of a transaction.
    * @param executionContext The execution context for ledger state operations.
    * @return The submission result (asynchronous).
    */
  def conflictDetectAndFinalize(
      now: () => Instant,
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairsWithLogEntry],
      ledgerStateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    val keys = preExecutionOutput.readSet.map(_._1)
    val preExecutionFingerprints = preExecutionOutput.readSet.map(_._2)

    for {
      values <- ledgerStateOperations.readState(keys)
      currentFingerprints = values.map(valueToFingerprint)
      hasConflict = preExecutionFingerprints != currentFingerprints
      result <- if (hasConflict)
        throw ConflictDetectedException
      else
        finalizeSubmission(now, preExecutionOutput, ledgerStateOperations)
    } yield result
  }
}

object PostExecutionFinalizer {
  val ConflictDetectedException = new RuntimeException(
    "A conflict has been detected with other submissions during post-execution")

  private def respectsTimeBounds(
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairsWithLogEntry],
      recordTime: Instant): Boolean =
    !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
      !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))

  private def finalizeSubmission[LogResult](
      now: () => Instant,
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairsWithLogEntry],
      ledgerStateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    val recordTime = now()
    val withinTimeBounds = respectsTimeBounds(preExecutionOutput, recordTime)
    val writeSet = createWriteSet(preExecutionOutput, withinTimeBounds)
    val logEntry = createLogEntry(preExecutionOutput, withinTimeBounds)
    for {
      _ <- ledgerStateOperations.writeState(writeSet)
      _ <- ledgerStateOperations.appendToLog(logEntry._1, logEntry._2)
    } yield SubmissionResult.Acknowledged
  }

  private def createLogEntry(
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairsWithLogEntry],
      withinTimeBounds: Boolean): (Bytes, Bytes) = {
    val writeSet = if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
    } else {
      preExecutionOutput.outOfTimeBoundsWriteSet
    }
    writeSet.logEntryKey -> writeSet.logEntryValue
  }

  private def createWriteSet(
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairsWithLogEntry],
      withinTimeBounds: Boolean): Seq[(Bytes, Bytes)] =
    if (withinTimeBounds) {
      preExecutionOutput.successWriteSet.state
    } else {
      Seq.empty
    }
}
