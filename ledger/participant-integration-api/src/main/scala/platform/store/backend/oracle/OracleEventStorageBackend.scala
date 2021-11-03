// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.sql.Connection

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.ledger.offset.Offset
import com.daml.platform.store.backend.common.{
  EventStorageBackendTemplate,
  ParameterStorageBackendTemplate,
}

object OracleEventStorageBackend
    extends EventStorageBackendTemplate(
      eventStrategy = OracleEventStrategy,
      queryStrategy = OracleQueryStrategy,
      participantAllDivulgedContractsPrunedUpToInclusive =
        ParameterStorageBackendTemplate.participantAllDivulgedContractsPrunedUpToInclusive,
    ) {

  def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""SELECT max(max_esi) FROM (
           (
               SELECT max(event_sequential_id) AS max_esi FROM participant_events_consuming_exercise
               WHERE event_offset = (select max(event_offset) from participant_events_consuming_exercise where event_offset <= $offset)
           ) UNION ALL (
               SELECT max(event_sequential_id) AS max_esi FROM participant_events_create
               WHERE event_offset = (select max(event_offset) from participant_events_create where event_offset <= $offset)
           ) UNION ALL (
               SELECT max(event_sequential_id) AS max_esi FROM participant_events_non_consuming_exercise
               WHERE event_offset = (select max(event_offset) from participant_events_non_consuming_exercise where event_offset <= $offset)
           )
       )"""
      .as(get[Long](1).?.single)(connection)
  }

  // Migration from mutable schema is not supported for Oracle
  override def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean = true

}
