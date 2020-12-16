// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.FingerprintedReadSet
import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

object FingerprintAwarePostExecutionConflictDetector
    extends PostExecutionConflictDetector[
      DamlStateKey,
      Fingerprint,
      FingerprintedReadSet,
      Any,
    ] {
  override def detectConflicts(
      preExecutionOutput: PreExecutionOutput[FingerprintedReadSet, Any],
      reader: StateReader[DamlStateKey, Fingerprint],
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val (keys, preExecutionFingerprints) = preExecutionOutput.readSet.unzip
    reader.read(keys).map { currentFingerprints =>
      if (preExecutionFingerprints != currentFingerprints) {
        throw new ConflictDetectedException
      }
    }
  }
}
