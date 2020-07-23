// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.LedgerStateAccess
import com.daml.ledger.validator.LedgerStateOperations.Value
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs

import scala.concurrent.{ExecutionContext, Future}

class PostExecutingStateAccessPersistStrategy[LogResult](
    valueToFingerprint: Option[Value] => Fingerprint) {

  import PostExecutingStateAccessPersistStrategy._

  def conflictDetectAndPersist(
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
          val recordTime = Instant.now()
          ledgerStateOperations
            .writeState(if (respectsTimeBounds(preExecutionOutput, recordTime)) {
              preExecutionOutput.successWriteSet
            } else {
              preExecutionOutput.outOfTimeBoundsWriteSet
            })
            .transform(_ => SubmissionResult.Acknowledged, identity)
        }
      }
    }
}

object PostExecutingStateAccessPersistStrategy {
  val Conflict = new RuntimeException

  private def respectsTimeBounds(
      preExecutionOutput: PreExecutionOutput[RawKeyValuePairs],
      recordTime: Instant): Boolean =
    !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
      !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))
}
