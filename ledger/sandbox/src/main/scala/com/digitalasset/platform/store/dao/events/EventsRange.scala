// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.ledger.participant.state.v1.Offset

final case class EventsRange[A](startExclusive: A, endInclusive: A)

object EventsRange {
  private val EmptyEventSeqIdRange = EventsRange(0L, 0L)

  /**
    * Converts Offset range to Event Sequential ID range.
    *
    * @param range offset range
    * @param connection SQL connection
    * @return Event Sequential ID range
    */
  def readEventSeqIdRange(range: EventsRange[Offset])(
      connection: java.sql.Connection): EventsRange[Long] =
    EventsRange(
      startExclusive = readLowerBound(range.startExclusive)(connection),
      endInclusive = readUpperBound(range.endInclusive)(connection)
    )

  /**
    * Converts ledger end offset to Event Sequential ID.
    *
    * @param endInclusive ledger end offset
    * @param connection SQL connection
    * @return Event Sequential ID range.
    */
  def readEventSeqIdRange(endInclusive: Offset)(
      connection: java.sql.Connection): EventsRange[Long] =
    EmptyEventSeqIdRange.copy(endInclusive = readUpperBound(endInclusive)(connection))

  private def readLowerBound(startExclusive: Offset)(connection: java.sql.Connection): Long =
    if (startExclusive == Offset.beforeBegin) {
      EmptyEventSeqIdRange.startExclusive
    } else {
      import com.daml.platform.store.Conversions.OffsetToStatement
      // This query could be: "select min(event_sequential_id) - 1 from participant_events where event_offset > ${range.startExclusive}"
      // however there are cases when postgres decides not to use the index. We are forcing the index usage specifying `order by event_offset`
      SQL"select min(event_sequential_id) from participant_events where event_offset > ${startExclusive} group by event_offset order by event_offset asc limit 1"
        .as(get[Long](1).singleOpt)(connection)
        .map(_ - 1L)
        .getOrElse(EmptyEventSeqIdRange.startExclusive)
    }

  private def readUpperBound(endInclusive: Offset)(connection: java.sql.Connection): Long =
    if (endInclusive == Offset.beforeBegin) {
      EmptyEventSeqIdRange.endInclusive
    } else {
      import com.daml.platform.store.Conversions.OffsetToStatement
      // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
      // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
      // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
      SQL"select max(event_sequential_id) from participant_events where event_offset <= ${endInclusive} group by event_offset order by event_offset desc limit 1"
        .as(get[Long](1).singleOpt)(connection)
        .getOrElse(EmptyEventSeqIdRange.endInclusive)
    }
}
