// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.validator.LedgerStateWriteOperations
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

final class SubmissionAggregatorWriteOperations(builder: SubmissionAggregator.WriteSetBuilder)
    extends LedgerStateWriteOperations[Unit] {

  override def writeState(
      key: Raw.StateKey,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    Future {
      builder += key -> value
    }

  override def writeState(
      keyValuePairs: Iterable[Raw.StateEntry]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    Future {
      builder ++= keyValuePairs
    }

  override def appendToLog(
      key: Raw.LogEntryId,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    Future {
      builder += key -> value
    }
}
