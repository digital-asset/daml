// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Raw

import scala.concurrent.{ExecutionContext, Future}

final class CombinedLedgerStateWriteOperations[ALogResult, BLogResult, LogResult](
    a: LedgerStateWriteOperations[ALogResult],
    b: LedgerStateWriteOperations[BLogResult],
    combineLogResults: (ALogResult, BLogResult) => LogResult,
) extends LedgerStateWriteOperations[LogResult] {

  override def writeState(
      key: Raw.Key,
      value: Raw.Value,
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    inParallel(a.writeState(key, value), b.writeState(key, value)).map(_ => ())

  override def writeState(
      keyValuePairs: Iterable[(Raw.Key, Raw.Value)]
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    inParallel(a.writeState(keyValuePairs), b.writeState(keyValuePairs)).map(_ => ())

  override def appendToLog(
      key: Raw.Key,
      value: Raw.Value,
  )(implicit executionContext: ExecutionContext): Future[LogResult] =
    inParallel(a.appendToLog(key, value), b.appendToLog(key, value)).map {
      case (aResult, bResult) => combineLogResults(aResult, bResult)
    }
}
