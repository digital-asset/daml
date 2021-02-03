// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.LedgerStateOperations

import scala.concurrent.{ExecutionContext, Future}

/** A writer that simply pushes the data to [[LedgerStateOperations]]. */
final class RawPostExecutionWriter extends PostExecutionWriter[RawKeyValuePairsWithLogEntry] {
  override def write[LogResult](
      writeSet: RawKeyValuePairsWithLogEntry,
      ledgerStateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] = {
    for {
      _ <- ledgerStateOperations.writeState(writeSet.state)
      _ <- ledgerStateOperations.appendToLog(writeSet.logEntryKey, writeSet.logEntryValue)
    } yield SubmissionResult.Acknowledged
  }
}
