// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.{Bytes, Err, Fingerprint}
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.{LedgerStateAccess, StateKeySerializationStrategy}
import com.daml.ledger.validator.LedgerStateOperations.Value
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs

import scala.concurrent.{ExecutionContext, Future}

class PostExecutingStateAccessPersistStrategy[LogResult](
    valueToFingerprint: Option[Value] => Fingerprint) {

  import PostExecutingStateAccessPersistStrategy._

  def conflictDetectAndPersist(
      now: () => Instant,
      keySerializationStrategy: StateKeySerializationStrategy,
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairs],
      ledgerStateAccess: LedgerStateAccess[LogResult])(
      implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    ledgerStateAccess.inTransaction { ledgerStateOperations =>
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

object PostExecutingStateAccessPersistStrategy {
  val Conflict = new RuntimeException

  private def isLogEntrySerializedKey(key: Bytes): Boolean =
    LogAppenderPreExecutingCommitStrategy.isPrefixedSerializedLogEntryId(key)

  private def respectsTimeBounds(
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairs],
      recordTime: Instant): Boolean =
    !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
      !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))
}
