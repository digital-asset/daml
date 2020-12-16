// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet
import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

object FingerprintAwarePostExecutionConflictDetector
    extends PostExecutionConflictDetector[
      Key,
      (Option[Value], Fingerprint),
      ReadSet,
      RawKeyValuePairsWithLogEntry,
    ] {
  override def detectConflicts(
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
