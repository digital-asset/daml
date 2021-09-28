// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

final class CombinedLedgerStateWriteOperations[ALogResult, BLogResult, LogResult](
    a: LedgerStateWriteOperations[ALogResult],
    b: LedgerStateWriteOperations[BLogResult],
    combineLogResults: (ALogResult, BLogResult) => LogResult,
) extends LedgerStateWriteOperations[LogResult] {

  override def writeState(
      key: Raw.StateKey,
      value: Raw.Envelope,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    inParallel(a.writeState(key, value), b.writeState(key, value)).map(_ => ())

  override def writeState(
      keyValuePairs: Iterable[Raw.StateEntry]
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] =
    inParallel(a.writeState(keyValuePairs), b.writeState(keyValuePairs)).map(_ => ())

  override def appendToLog(
      key: Raw.LogEntryId,
      value: Raw.Envelope,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[LogResult] =
    inParallel(a.appendToLog(key, value), b.appendToLog(key, value)).map {
      case (aResult, bResult) => combineLogResults(aResult, bResult)
    }
}
