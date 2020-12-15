// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

/**
  * An in-transaction post-execution conflict detection to be invoked as the last stage of a
  * pre-execution pipeline.
  */
trait PostExecutionConflictDetector[StateKey, StateValue, ReadSet, WriteSet] {

  /**
    * @param preExecutionOutput    The output from the pre-execution stage.
    * @param ledgerStateOperations The operations that can access actual ledger storage as part of a transaction.
    * @param executionContext      The execution context for ledger state operations.
    * @return The submission result (asynchronous).
    */
  def detectConflicts(
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet],
      ledgerStateOperations: StateReader[StateKey, StateValue],
  )(implicit executionContext: ExecutionContext): Future[Unit]
}
