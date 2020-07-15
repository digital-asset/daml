// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.metrics.{Metrics, Timed}

import scala.collection.immutable
import scala.util.Try

class TimedReadQueries(delegate: ReadQueries, metrics: Metrics) extends ReadQueries {

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

  override def selectStateValuesByKeys(keys: Seq[Key]): Try[immutable.Seq[Option[Value]]] =
    Timed.value(
      metrics.daml.ledger.database.queries.selectStateValuesByKeys,
      delegate.selectStateValuesByKeys(keys),
    )

}
