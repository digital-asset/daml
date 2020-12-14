// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.LedgerStateOperations

import scala.concurrent.{ExecutionContext, Future}

/**
  * An in-transasaction finalizer that persists both the ledger state and the log entry after all
  * checks have been performed.
  */
final class RawPostExecutionFinalizer[ReadSet](now: () => Instant)
    extends PostExecutionFinalizer[ReadSet, RawKeyValuePairsWithLogEntry] {
  override def finalizeSubmission[LogResult](
      preExecutionOutput: PreExecutionOutput[ReadSet, RawKeyValuePairsWithLogEntry],
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

  private def respectsTimeBounds(
      preExecutionOutput: PreExecutionOutput[Any, Any],
      recordTime: Instant,
  ): Boolean =
    !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
      !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))

  private def createLogEntry(
      preExecutionOutput: PreExecutionOutput[Any, RawKeyValuePairsWithLogEntry],
      withinTimeBounds: Boolean,
  ): (Bytes, Bytes) = {
    val writeSet = if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
    } else {
      preExecutionOutput.outOfTimeBoundsWriteSet
    }
    writeSet.logEntryKey -> writeSet.logEntryValue
  }

  private def createWriteSet(
      preExecutionOutput: PreExecutionOutput[Any, RawKeyValuePairsWithLogEntry],
      withinTimeBounds: Boolean,
  ): Iterable[(Bytes, Bytes)] =
    if (withinTimeBounds) {
      preExecutionOutput.successWriteSet.state
    } else {
      Seq.empty
    }
}
