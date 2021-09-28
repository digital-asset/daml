// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.validator.reading.StateReader
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

/** An in-transaction post-execution conflict detection to be invoked as the last stage of a
  * pre-execution pipeline.
  */
trait PostExecutionConflictDetector[StateKey, StateValue, -ReadSet, -WriteSet] {
  self =>

  /** Re-reads the current state of the ledger and ensures that the state has not changed compared
    * to the pre-execution read set.
    *
    * If there is a conflict, throws a [[ConflictDetectedException]].
    *
    * @param preExecutionOutput The output from the pre-execution stage.
    * @param reader             The operations that can access actual ledger storage as part of a transaction.
    * @param executionContext   The execution context for ledger state operations.
    * @return The submission result (asynchronous).
    */
  def detectConflicts(
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet],
      reader: StateReader[StateKey, StateValue],
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit]

  /** Transforms the reader to widen the state value, allowing it to handle any value that can be
    * converted to `StateValue`.
    *
    * This is an instance of a "contravariant functor", in which the mapping is backwards, because
    * the values are used as input to the conflict detection.
    *
    * @tparam NewStateValue The new state value type.
    * @return A new conflict detector that transforms after reading, before detecting conflicts.
    */
  def contramapValues[NewStateValue](
      transformValue: NewStateValue => StateValue
  ): PostExecutionConflictDetector[StateKey, NewStateValue, ReadSet, WriteSet] =
    new PostExecutionConflictDetector[StateKey, NewStateValue, ReadSet, WriteSet] {
      override def detectConflicts(
          preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet],
          reader: StateReader[StateKey, NewStateValue],
      )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
        self.detectConflicts(preExecutionOutput, reader.mapValues(transformValue))
    }
}
