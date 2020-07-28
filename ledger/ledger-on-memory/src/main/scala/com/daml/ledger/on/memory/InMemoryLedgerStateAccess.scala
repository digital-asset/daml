// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.validator.{
  LedgerStateAccess,
  LedgerStateOperations,
  TimedLedgerStateOperations
}
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

private[memory] class InMemoryLedgerStateAccess(state: InMemoryState, metrics: Metrics)(
    implicit executionContext: ExecutionContext)
    extends LedgerStateAccess[Index] {
  override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
    state.write { (log, state) =>
      body(new TimedLedgerStateOperations(new InMemoryLedgerStateOperations(log, state), metrics))
    }
}
