// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.validator.reading.StateReader
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

final class EqualityBasedPostExecutionConflictDetector[StateKey, StateValue]
    extends PostExecutionConflictDetector[StateKey, StateValue, Map[StateKey, StateValue], Any] {
  override def detectConflicts(
      preExecutionOutput: PreExecutionOutput[Map[StateKey, StateValue], Any],
      reader: StateReader[StateKey, StateValue],
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] = {
    val (keys, preExecutionValues) = preExecutionOutput.readSet.unzip
    reader.read(keys).map { currentValues =>
      if (preExecutionValues != currentValues) {
        throw new ConflictDetectedException
      }
    }
  }
}
