// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.long
import anorm.{RowParser, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.Offset

final case class EventsRange[A](startExclusive: A, endInclusive: A)

object EventsRange {
  val parser: RowParser[EventsRange[Long]] =
    (long("start") ~ long("end")).map(r => EventsRange(r._1, r._2))

  /**
    * Converts Offset range to Row ID range.
    *
    * @param range offset range
    * @param connection SQL connection
    * @return Row ID range
    */
  def eventsRange(range: EventsRange[Offset])(
      implicit connection: java.sql.Connection): EventsRange[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    val query =
      SQL"select min(row_id) as start, max(row_id) as end from participant_events where event_offset > ${range.startExclusive} and event_offset <= ${range.endInclusive}"

    query.as(parser.single)
  }
}
