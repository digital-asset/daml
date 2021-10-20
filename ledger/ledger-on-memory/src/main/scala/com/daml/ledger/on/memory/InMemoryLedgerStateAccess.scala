// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.KVOffsetBuilder
import com.daml.ledger.validator.{
  LedgerStateAccess,
  LedgerStateOperations,
  TimedLedgerStateOperations,
}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

final class InMemoryLedgerStateAccess(
    offsetBuilder: KVOffsetBuilder,
    state: InMemoryState,
    metrics: Metrics,
) extends LedgerStateAccess[Index] {
  override def inTransaction[T](
      body: LedgerStateOperations[Index] => Future[T]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[T] =
    state.write { (log, state) =>
      body(
        new TimedLedgerStateOperations(
          new InMemoryLedgerStateOperations(offsetBuilder, log, state),
          metrics,
        )
      )
    }
}
