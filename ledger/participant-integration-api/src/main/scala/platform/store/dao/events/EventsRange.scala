// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.store.DbType
import com.daml.platform.store.DbType.Oracle

// (startExclusive, endInclusive]
private[events] final case class EventsRange[A](startExclusive: A, endInclusive: A) {
  def map[B](f: A => B): EventsRange[B] =
    copy(startExclusive = f(startExclusive), endInclusive = f(endInclusive))
}

private[events] object EventsRange {
  private val EmptyLedgerEventSeqId = 0L

  // (0, 0] -- non-existent range
  private val EmptyEventSeqIdRange = EventsRange(EmptyLedgerEventSeqId, EmptyLedgerEventSeqId)

  def isEmpty[A: Ordering](range: EventsRange[A]): Boolean = {
    val A = implicitly[Ordering[A]]
    A.gteq(range.startExclusive, range.endInclusive)
  }

  /** Converts Offset range to Event Sequential ID range.
    *
    * @param range offset range
    * @param connection SQL connection
    * @return Event Sequential ID range
    */
  def readEventSeqIdRange(range: EventsRange[Offset], dbType: DbType)(
      connection: java.sql.Connection
  ): EventsRange[Long] =
    if (isEmpty(range))
      EmptyEventSeqIdRange
    else
      EventsRange(
        startExclusive = readEventSeqId(range.startExclusive, dbType)(connection),
        endInclusive = readEventSeqId(range.endInclusive, dbType)(connection),
      )

  /** Converts ledger end offset to Event Sequential ID.
    *
    * @param endInclusive ledger end offset
    * @param connection SQL connection
    * @return Event Sequential ID range.
    */
  def readEventSeqIdRange(endInclusive: Offset, dbType: DbType)(
      connection: java.sql.Connection
  ): EventsRange[Long] = {
    if (endInclusive == Offset.beforeBegin) EmptyEventSeqIdRange
    else EmptyEventSeqIdRange.copy(endInclusive = readEventSeqId(endInclusive, dbType)(connection))
  }

  private def readEventSeqId(offset: Offset, dbType: DbType)(
      connection: java.sql.Connection
  ): Long = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    // This query could be: "select max(event_sequential_id) from participant_events where event_offset <= ${range.endInclusive}"
    // however tests using PostgreSQL 12 with tens of millions of events have shown that the index
    // on `event_offset` is not used unless we _hint_ at it by specifying `order by event_offset`
    val query = dbType match {
      // 2 - Need to figure out order by event_offset alternative
      // 3 - Need to get group by to work field even works
      case Oracle =>
        SQL"select nvl(max(event_sequential_id),0) from participant_events where (dbms_lob.compare(event_offset, ${offset}) IN (0, -1)) fetch next 1 rows only"
      case _ =>
        SQL"select max(event_sequential_id) from participant_events where event_offset <= ${offset} group by event_offset order by event_offset desc limit 1"
    }

    query
      .as(get[Long](1).singleOpt)(connection)
      .getOrElse(EmptyLedgerEventSeqId)
  }

  private[events] def readPage[A](
      read: (EventsRange[Long], String) => SimpleSql[Row], // takes range and limit sub-expression
      row: RowParser[A],
      range: EventsRange[Long],
      pageSize: Int,
  ): SqlSequence[Vector[A]] = {
    val minPageSize = 10 min pageSize max (pageSize / 10)
    val guessedPageEnd = range.endInclusive min (range.startExclusive + pageSize)
    SqlSequence
      .vector(
        read(range copy (endInclusive = guessedPageEnd), "") withFetchSize Some(pageSize),
        row,
      )
      .flatMap { arithPage =>
        val found = arithPage.size
        if (guessedPageEnd == range.endInclusive || found >= minPageSize)
          SqlSequence point arithPage
        else
          SqlSequence
            .vector(
              read(
                range copy (startExclusive = guessedPageEnd),
                s"limit ${minPageSize - found: Int}",
              ) withFetchSize Some(minPageSize - found),
              row,
            )
            .map(arithPage ++ _)
      }
  }
}
