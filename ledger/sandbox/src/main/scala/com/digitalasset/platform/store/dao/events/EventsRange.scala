// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.ledger.participant.state.v1.Offset

// (startExclusive, endInclusive]
final case class EventsRange[A](startExclusive: A, endInclusive: A)

object EventsRange {
  private val EmptyLedgerEventSeqId = 0L

  // (0, 0] -- non-existent range
  private val EmptyEventSeqIdRange = EventsRange(EmptyLedgerEventSeqId, EmptyLedgerEventSeqId)

  def isEmpty[A: Ordering](range: EventsRange[A]): Boolean = {
    val A = implicitly[Ordering[A]]
    A.gteq(range.startExclusive, range.endInclusive)
  }

  /**
    * Converts Offset range to Event Sequential ID range.
    *
    * @param range offset range
    * @param connection SQL connection
    * @return Event Sequential ID range
    */
  def readEventSeqIdRange(range: EventsRange[Offset])(
      connection: java.sql.Connection
  ): EventsRange[Long] =
    if (isEmpty(range)) EmptyEventSeqIdRange
    else readEventSeqIdRangeOpt(range)(connection).getOrElse(EmptyEventSeqIdRange)

  private def readEventSeqIdRangeOpt(range: EventsRange[Offset])(
      connection: java.sql.Connection
  ): Option[EventsRange[Long]] =
    for {
      startExclusive <- readEventSeqId(range.startExclusive)(connection)
      endInclusive <- readEventSeqId(range.endInclusive)(connection)
    } yield EventsRange(startExclusive = startExclusive, endInclusive = endInclusive)

  /**
    * Converts ledger end offset to Event Sequential ID.
    *
    * @param endInclusive ledger end offset
    * @param connection SQL connection
    * @return Event Sequential ID range.
    */
  def readEventSeqIdRange(endInclusive: Offset)(
      connection: java.sql.Connection
  ): EventsRange[Long] = {
    if (endInclusive == Offset.beforeBegin)
      EmptyEventSeqIdRange
    else
      readEventSeqId(endInclusive)(connection)
        .fold(EmptyEventSeqIdRange)(x => EmptyEventSeqIdRange.copy(endInclusive = x))
  }

  private def readEventSeqId(offset: Offset)(connection: java.sql.Connection): Option[Long] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    SQL"select max(event_sequential_id) from participant_events where event_offset <= ${offset} group by event_offset order by event_offset desc limit 1"
      .as(get[Long](1).singleOpt)(connection)
  }
}
