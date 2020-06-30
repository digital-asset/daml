// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.ledger.participant.state.v1.Offset

final case class EventsRange[A](startExclusive: A, endInclusive: A)

object EventsRange {
  private val EmptyRowIdRange = EventsRange(0L, 0L)

  private val oneRow = Some(1)

  /**
    * Converts Offset range to Row ID range.
    *
    * @param range offset range
    * @param connection SQL connection
    * @return Row ID range
    */
  def readRowIdRange(range: EventsRange[Offset])(
      connection: java.sql.Connection): EventsRange[Long] =
    EventsRange(
      startExclusive = readLowerBound(range.startExclusive)(connection),
      endInclusive = readUpperBound(range.endInclusive)(connection)
    )

  /**
    * Converts ledger end offset into a Row ID range.
    *
    * @param endInclusive ledger end offset
    * @param connection SQL connection
    * @return Row ID range
    */
  def readRowIdRange(endInclusive: Offset)(connection: java.sql.Connection): EventsRange[Long] =
    EmptyRowIdRange.copy(endInclusive = readUpperBound(endInclusive)(connection))

  private def readLowerBound(startExclusive: Offset)(connection: java.sql.Connection): Long =
    if (startExclusive == Offset.beforeBegin) {
      EmptyRowIdRange.startExclusive
    } else {
      import com.daml.platform.store.Conversions.OffsetToStatement
      // This query could be: "select min(row_id) - 1 as start from participant_events where event_offset > ${range.startExclusive}"
      // however there are cases when postgres decides not to use the index. We are forcing the index usage specifying `order by event_offset`
      SQL"select min(row_id) from participant_events where event_offset > ${startExclusive} group by event_offset order by event_offset asc limit 1"
        .withFetchSize(oneRow)
        .as(get[Option[Long]](1).single)(connection)
        .map(_ - 1L)
        .getOrElse(EmptyRowIdRange.startExclusive)
    }

  private def readUpperBound(endInclusive: Offset)(connection: java.sql.Connection): Long =
    if (endInclusive == Offset.beforeBegin) {
      EmptyRowIdRange.endInclusive
    } else {
      import com.daml.platform.store.Conversions.OffsetToStatement
      // This query could be: "select max(row_id) as end from participant_events where event_offset <= ${range.endInclusive}"
      // however there are cases (query takes 24 min on DB with 4million participant_events rows)
      // when postgres decides not to use the index. We are forcing the index usage specifying `order by event_offset`
      SQL"select max(row_id) from participant_events where event_offset <= ${endInclusive} group by event_offset order by event_offset desc limit 1"
        .withFetchSize(oneRow)
        .as(get[Option[Long]](1).single)(connection)
        .getOrElse(EmptyRowIdRange.endInclusive)
    }
}
