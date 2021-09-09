// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.validator.LedgerStateWriteOperations

import scala.concurrent.{ExecutionContext, Future}

/** A writer that simply pushes the data to [[LedgerStateWriteOperations]]. */
final class RawPostExecutionWriter extends PostExecutionWriter[RawKeyValuePairsWithLogEntry] {
  override def write[LogResult](
      writeSet: RawKeyValuePairsWithLogEntry,
      operations: LedgerStateWriteOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    for {
      _ <- operations.writeState(writeSet.state)
      _ <- operations.appendToLog(writeSet.logEntryKey, writeSet.logEntryValue)
    } yield SubmissionResult.Acknowledged
  }
}
