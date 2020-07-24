// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.{Bytes, Err, Fingerprint}
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.LedgerStateOperations
import com.daml.ledger.validator.LedgerStateOperations.Value
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs

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
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairs],
      ledgerStateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    val keys = preExecutionOutput.readSet.map(_._1)
    val preExecutionFingerprints = preExecutionOutput.readSet.map(_._2)

    val conflictFuture = for {
      values <- ledgerStateOperations.readState(keys)
      postExecutionFingerprints = values.map(valueToFingerprint)
    } yield preExecutionFingerprints != postExecutionFingerprints

    conflictFuture.flatMap { hasConflict =>
      if (hasConflict) {
        throw Conflict
      } else {
        val recordTime = now()
        val withinTimeBounds = respectsTimeBounds(preExecutionOutput, recordTime)
        val writeSet = if (withinTimeBounds) {
          preExecutionOutput.successWriteSet.filter {
            case (key, _) =>
              !isLogEntrySerializedKey(key)
          }
        } else {
          Seq.empty
        }
        val logEntry = if (withinTimeBounds) {
          preExecutionOutput.successWriteSet
            .find {
              case (key, _) =>
                isLogEntrySerializedKey(key)
            }
            .getOrElse(throw Err.DecodeError(
              "Pre-execution result",
              "A log entry must always be present in the success write set for pre-execution but its not"))
        } else {
          preExecutionOutput.outOfTimeBoundsWriteSet.head
        }
        for {
          _ <- ledgerStateOperations.writeState(writeSet)
          _ <- ledgerStateOperations.appendToLog(
            LogAppenderPreExecutingCommitStrategy.unprefixSerializedLogEntryId(logEntry._1),
            logEntry._2)
        } yield SubmissionResult.Acknowledged
      }
    }
  }
}

object PostExecutionFinalizerWithFingerprintsFromValues {
  val Conflict = new RuntimeException

  private def isLogEntrySerializedKey(key: Bytes): Boolean =
    LogAppenderPreExecutingCommitStrategy.isPrefixedSerializedLogEntryId(key)

  private def respectsTimeBounds(
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairs],
      recordTime: Instant): Boolean =
    !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
      !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))
}
