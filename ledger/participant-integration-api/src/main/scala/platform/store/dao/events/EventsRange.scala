// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.{DbType, EventSequentialId}

// (startExclusive, endInclusive]
private[events] final case class EventsRange[A](startExclusive: A, endInclusive: A) {
  def map[B](f: A => B): EventsRange[B] =
    copy(startExclusive = f(startExclusive), endInclusive = f(endInclusive))
}

private[events] object EventsRange {

  // (0, 0] -- non-existent range
  private val EmptyEventSeqIdRange = EventsRange(EventSequentialId.zero, EventSequentialId.zero)

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

    SQL"select max(event_sequential_id) from participant_events where event_offset <= ${offset} group by event_offset order by event_offset desc #${SqlFunctions(dbType)
      .limitClause(1)}"
      .as(get[Long](1).singleOpt)(connection)
      .getOrElse(EventSequentialId.zero)
  }

  private[events] def readPage[A](
      read: (
          EventsRange[Long],
          Option[Int],
      ) => SimpleSql[Row], // takes range and limit sub-expression
      row: RowParser[A],
      range: EventsRange[Long],
      pageSize: Int,
  ): SqlSequence[Vector[A]] = {
    val minPageSize = 10 min pageSize max (pageSize / 10)
    val guessedPageEnd = range.endInclusive min (range.startExclusive + pageSize)
    SqlSequence
      .vector(
        read(range copy (endInclusive = guessedPageEnd), None) withFetchSize Some(pageSize),
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
                Some(minPageSize - found),
              ) withFetchSize Some(minPageSize - found),
              row,
            )
            .map(arithPage ++ _)
      }
  }
}
