// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

final class EqualityBasedPostExecutionConflictDetector[StateValue]
    extends PostExecutionConflictDetector[
      DamlStateKey,
      StateValue,
      Map[DamlStateKey, StateValue],
      Any,
    ] {
  override def detectConflicts(
      preExecutionOutput: PreExecutionOutput[Map[DamlStateKey, StateValue], Any],
      reader: StateReader[DamlStateKey, StateValue],
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val (keys, preExecutionValues) = preExecutionOutput.readSet.unzip
    reader.read(keys).map { currentValues =>
      if (preExecutionValues != currentValues) {
        throw new ConflictDetectedException
      }
    }
  }
}
