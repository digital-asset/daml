// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.validator.LedgerStateWriteOperations
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

/** A writer that simply pushes the data to [[LedgerStateWriteOperations]]. */
final class RawPostExecutionWriter extends PostExecutionWriter[RawKeyValuePairsWithLogEntry] {
  override def write[LogResult](
      writeSet: RawKeyValuePairsWithLogEntry,
      operations: LedgerStateWriteOperations[LogResult],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[SubmissionResult] = {
    for {
      _ <- operations.writeState(writeSet.state)
      _ <- operations.appendToLog(writeSet.logEntryKey, writeSet.logEntryValue)
    } yield SubmissionResult.Acknowledged
  }
}
