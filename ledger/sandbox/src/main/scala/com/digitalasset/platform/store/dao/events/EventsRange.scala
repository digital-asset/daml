// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.{RowParser, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.Offset

final case class EventsRange[A](startExclusive: A, endInclusive: A)

object EventsRange {
  private val EmptyRowIdRange = EventsRange(0L, 0L)

  private val rangeParser: RowParser[EventsRange[Long]] =
    (get[Option[Long]]("start") ~ get[Option[Long]]("end")).map { row =>
      EventsRange(
        startExclusive = row._1.getOrElse(EmptyRowIdRange.startExclusive),
        endInclusive = row._2.getOrElse(EmptyRowIdRange.endInclusive))
    }

  private val endParser: RowParser[EventsRange[Long]] =
    get[Option[Long]]("end")
      .map(_.fold(EmptyRowIdRange)(x => EmptyRowIdRange.copy(endInclusive = x)))

  /**
    * Converts Offset range to Row ID range.
    *
    * @param range offset range
    * @param connection SQL connection
    * @return Row ID range
    */
  def readRowIdRange(range: EventsRange[Offset])(
      connection: java.sql.Connection): EventsRange[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    if (range.startExclusive == Offset.beforeBegin) {
      readRowIdRange(range.endInclusive)(connection)
    } else {
      // start is exclusive, that is why -1
      val query =
        SQL"select min(row_id) - 1 as start, max(row_id) as end from participant_events where event_offset > ${range.startExclusive} and event_offset <= ${range.endInclusive}"
      query.as(rangeParser.single)(connection)
    }
  }

  /**
    * Converts ledger end offset into a Row ID range.
    *
    * @param endInclusive ledger end offset
    * @param connection SQL connection
    * @return Row ID range
    */
  def readRowIdRange(endInclusive: Offset)(connection: java.sql.Connection): EventsRange[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    if (endInclusive == Offset.beforeBegin) {
      EmptyRowIdRange
    } else {
      val query =
        SQL"select max(row_id) as end from participant_events where event_offset <= ${endInclusive}"
      query.as(endParser.single)(connection)
    }
  }
}
