// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.metrics.{MetricName, Timed}

import scala.collection.immutable
import scala.util.Try

final class TimedQueries(delegate: Queries, metricRegistry: MetricRegistry) extends Queries {

  override def selectLatestLogEntryId(): Try[Option[Index]] =
    Timed.value(Metrics.selectLatestLogEntryId, delegate.selectLatestLogEntryId())

  override def selectFromLog(start: Index, end: Index): Try[immutable.Seq[(Index, LedgerRecord)]] =
    Timed.value(Metrics.selectFromLog, delegate.selectFromLog(start, end))

  override def selectStateValuesByKeys(keys: Seq[Key]): Try[immutable.Seq[Option[Value]]] =
    Timed.value(Metrics.selectStateValuesByKeys, delegate.selectStateValuesByKeys(keys))

  override def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId] =
    Timed.value(
      Metrics.updateOrRetrieveLedgerId,
      delegate.updateOrRetrieveLedgerId(providedLedgerId))

  override def insertRecordIntoLog(key: Key, value: Value): Try[Index] =
    Timed.value(Metrics.insertRecordIntoLog, delegate.insertRecordIntoLog(key, value))

  override def updateState(stateUpdates: Seq[(Key, Value)]): Try[Unit] =
    Timed.value(Metrics.updateState, delegate.updateState(stateUpdates))

  private object Metrics {
    private val prefix = MetricName.DAML :+ "ledger" :+ "database" :+ "queries"

    val selectLatestLogEntryId: Timer =
      metricRegistry.timer(prefix :+ "select_latest_log_entry_id")
    val selectFromLog: Timer =
      metricRegistry.timer(prefix :+ "select_from_log")
    val selectStateValuesByKeys: Timer =
      metricRegistry.timer(prefix :+ "select_state_values_by_keys")
    val updateOrRetrieveLedgerId: Timer =
      metricRegistry.timer(prefix :+ "update_or_retrieve_ledger_id")
    val insertRecordIntoLog: Timer =
      metricRegistry.timer(prefix :+ "insert_record_into_log")
    val updateState: Timer =
      metricRegistry.timer(prefix :+ "update_state")
  }

}
