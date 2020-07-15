// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.metrics.{Metrics, Timed}

import scala.util.Try

final class TimedReadWriteQueries(delegate: ReadWriteQueries, metrics: Metrics)
    extends TimedReadQueries(delegate, metrics)
    with ReadWriteQueries {

  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId] =
    Timed.value(
      metrics.daml.ledger.database.queries.updateOrRetrieveLedgerId,
      delegate.updateOrRetrieveLedgerId(providedLedgerId),
    )

  override def insertRecordIntoLog(key: Key, value: Value): Try[Index] =
    Timed.value(
      metrics.daml.ledger.database.queries.insertRecordIntoLog,
      delegate.insertRecordIntoLog(key, value))

  override def updateState(stateUpdates: Seq[(Key, Value)]): Try[Unit] =
    Timed.value(
      metrics.daml.ledger.database.queries.updateState,
      delegate.updateState(stateUpdates),
    )

  override def truncate(): Try[Unit] =
    Timed.value(metrics.daml.ledger.database.queries.truncate, delegate.truncate())

}
