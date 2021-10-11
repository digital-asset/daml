// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.metrics.{Metrics, Timed}

import scala.collection.immutable
import scala.util.Try

final class TimedQueries(delegate: Queries, metrics: Metrics) extends Queries {

  override def selectLatestLogEntryId(): Try[Option[Index]] =
    Timed.value(
      metrics.daml.ledger.database.queries.selectLatestLogEntryId,
      delegate.selectLatestLogEntryId(),
    )

  override def selectFromLog(start: Index, end: Index): Try[immutable.Seq[(Index, LedgerRecord)]] =
    Timed.value(
      metrics.daml.ledger.database.queries.selectFromLog,
      delegate.selectFromLog(start, end),
    )

  override def selectStateValuesByKeys(
      keys: Iterable[Raw.StateKey]
  ): Try[immutable.Seq[Option[Raw.Envelope]]] =
    Timed.value(
      metrics.daml.ledger.database.queries.selectStateValuesByKeys,
      delegate.selectStateValuesByKeys(keys),
    )

  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId] =
    Timed.value(
      metrics.daml.ledger.database.queries.updateOrRetrieveLedgerId,
      delegate.updateOrRetrieveLedgerId(providedLedgerId),
    )

  override def insertRecordIntoLog(key: Raw.LogEntryId, value: Raw.Envelope): Try[Index] =
    Timed.value(
      metrics.daml.ledger.database.queries.insertRecordIntoLog,
      delegate.insertRecordIntoLog(key, value),
    )

  override def updateState(stateUpdates: Iterable[Raw.StateEntry]): Try[Unit] =
    Timed.value(
      metrics.daml.ledger.database.queries.updateState,
      delegate.updateState(stateUpdates),
    )

  override def truncate(): Try[Unit] =
    Timed.value(metrics.daml.ledger.database.queries.truncate, delegate.truncate())

}
