// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.{Bytes, Err, Fingerprint}
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
class PostExecutionFinalizerWithFingerprintsFromValues[LogResult](
    valueToFingerprint: Option[Value] => Fingerprint) {

  import PostExecutionFinalizerWithFingerprintsFromValues._

  /**
    * @param now The logic that produces the current instant for record time bounds check and record time finality.
    * @param preExecutionOutput The output from the pre-execution stage.
    * @param ledgerStateOperations The operations that can access actual ledger storage as part of a transaction.
    * @param executionContext The execution context for ledger state operations.
    * @return The submission result (asynchronous).
    */
  def conflictDetectAndFinalize(
      now: () => Instant,
      preExecutionOutput: PreExecutionOutput[AnnotatedRawKeyValuePairs],
      ledgerStateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    val keys = preExecutionOutput.readSet.map(_._1)
    val preExecutionFingerprints = preExecutionOutput.readSet.map(_._2)

    val conflictsWithCurrentState = for {
      values <- ledgerStateOperations.readState(keys)
      postExecutionFingerprints = values.map(valueToFingerprint)
    } yield preExecutionFingerprints != postExecutionFingerprints

    conflictsWithCurrentState.flatMap { hasConflict =>
      if (hasConflict)
        throw Conflict
      else
        finalizeSubmission(now, preExecutionOutput, ledgerStateOperations)
    }
  }
}

object PostExecutionFinalizerWithFingerprintsFromValues {
  val Conflict = new RuntimeException

  private def respectsTimeBounds(
      preExecutionOutput: PreExecutionOutput[AnnotatedRawKeyValuePairs],
      recordTime: Instant): Boolean =
    !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
      !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))

  private def finalizeSubmission[LogResult](
      now: () => Instant,
      preExecutionOutput: PreExecutionOutput[AnnotatedRawKeyValuePairs],
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
      preExecutionOutput: PreExecutionOutput[AnnotatedRawKeyValuePairs],
      withinTimeBounds: Boolean): (Bytes, Bytes) = {
    val annotatedRawKeyValuePair = if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
        .find {
          case (key, _) =>
            key.isLogEntry
        }
        .getOrElse(throw Err.DecodeError(
          "Pre-execution result",
          "A log entry must always be present in the success write set for pre-execution but its not"))
    } else {
      preExecutionOutput.outOfTimeBoundsWriteSet.head
    }
    annotatedToRawKeyValuePair(annotatedRawKeyValuePair)
  }

  private def createWriteSet(
      preExecutionOutput: PreExecutionOutput[AnnotatedRawKeyValuePairs],
      withinTimeBounds: Boolean): Seq[(Bytes, Bytes)] =
    if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
        .filter {
          case (key, _) =>
            !key.isLogEntry
        }
        .map(annotatedToRawKeyValuePair)
    } else {
      Seq.empty
    }

  private def annotatedToRawKeyValuePair(
      annotatedRawKeyValuePair: (AnnotatedRawKey, Bytes)): (Bytes, Bytes) =
    annotatedRawKeyValuePair._1.key -> annotatedRawKeyValuePair._2
}
