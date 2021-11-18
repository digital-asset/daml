// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

import anorm.SqlParser.{get, int}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.{
  EventStorageBackendTemplate,
  ParameterStorageBackendTemplate,
}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

class PostgresEventStorageBackend(ledgerEndCache: LedgerEndCache, stringInterning: StringInterning)
    extends EventStorageBackendTemplate(
      eventStrategy = PostgresEventStrategy,
      queryStrategy = PostgresQueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      participantAllDivulgedContractsPrunedUpToInclusive =
        ParameterStorageBackendTemplate.participantAllDivulgedContractsPrunedUpToInclusive,
    ) {

  // TODO FIXME: Use tables directly instead of the participant_events view.
  override def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= $offset group by event_offset order by event_offset desc limit 1"
      .as(get[Long](1).singleOpt)(connection)
  }

  /** If `pruneAllDivulgedContracts` is set, validate that the pruning offset is after
    * the last event offset that was ingested before the migration to append-only schema (if such event offset exists).
    * (see [[com.daml.platform.store.appendonlydao.JdbcLedgerDao.prune]])
    */
  override def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean =
    if (pruneAllDivulgedContracts) {
      import com.daml.platform.store.Conversions.OffsetToStatement
      SQL"""
       with max_offset_before_migration as (
         select max(event_offset) as max_event_offset
         from participant_events, participant_migration_history_v100
         where event_sequential_id <= ledger_end_sequential_id_before
       )
       select 1 as result
       from max_offset_before_migration
       where max_event_offset >= $pruneUpToInclusive
       """
        .as(int("result").singleOpt)(connection)
        .isEmpty
    } else {
      true
    }
}
