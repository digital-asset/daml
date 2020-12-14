// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.preexecution.PostExecutionConflictDetection._
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet
import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

/**
  * An in-transaction post-execution conflict detection to be invoked as the last stage of a
  * pre-execution pipeline. It performs deferred time bound checks and detects pre-execution
  * conflicts.
  */
class PostExecutionConflictDetection {

  /**
    * @param preExecutionOutput    The output from the pre-execution stage.
    * @param ledgerStateOperations The operations that can access actual ledger storage as part of a transaction.
    * @param executionContext      The execution context for ledger state operations.
    * @return The submission result (asynchronous).
    */
  def detectConflicts(
      preExecutionOutput: PreExecutionOutput[ReadSet, RawKeyValuePairsWithLogEntry],
      ledgerStateOperations: StateReader[Key, (Option[Value], Fingerprint)],
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val keys = preExecutionOutput.readSet.map(_._1)
    ledgerStateOperations.read(keys).map { values =>
      val preExecutionFingerprints = preExecutionOutput.readSet.map(_._2)
      val currentFingerprints = values.map(_._2)
      val hasConflict = preExecutionFingerprints != currentFingerprints
      if (hasConflict) {
        throw new ConflictDetectedException
      }
    }
  }
}

object PostExecutionConflictDetection {

  final class ConflictDetectedException
      extends RuntimeException(
        "A conflict has been detected with other submissions during post-execution.")

}
