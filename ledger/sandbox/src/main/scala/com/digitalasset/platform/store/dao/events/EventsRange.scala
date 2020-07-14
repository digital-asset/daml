// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import anorm.SqlParser.get
import anorm.SqlStringInterpolation
import com.daml.ledger.participant.state.v1.Offset

import com.daml.platform.store.Conversions.OffsetToStatement

// (startExclusive, endInclusive]
final case class EventsRange[A](startExclusive: A, endInclusive: A)

object EventsRange {
  // (0, 0] -- non-existent range
  private val EmptyEventSeqIdRange = EventsRange(0L, 0L)

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
      // TODO(Leo): fuse these two queries
      readMaxRowId(startExclusive)(connection).getOrElse(
        readNextRowId(startExclusive)(connection)
          .map(_ - 1L)
          .getOrElse(
            if (isLedgerEmpty(connection))
              EmptyEventSeqIdRange.startExclusive
            else
              throw new IllegalStateException(
                s"readLowerBound($startExclusive) returned None on non-empty ledger")
          )
      )
    }

  private def readMaxRowId(offset: Offset)(connection: java.sql.Connection): Option[Long] = {
    SQL"select max(event_sequential_id) from participant_events where event_offset = ${offset}"
      .as(get[Option[Long]](1).single)(connection)
  }

  private def readNextRowId(offset: Offset)(connection: java.sql.Connection): Option[Long] = {
    SQL"select min(event_sequential_id) from participant_events where event_offset > ${offset}"
      .as(get[Option[Long]](1).single)(connection)
  }

  private def readUpperBound(endInclusive: Offset)(connection: java.sql.Connection): Long =
    if (endInclusive == Offset.beforeBegin) {
      EmptyEventSeqIdRange.endInclusive
    } else {
      SQL"select max(event_sequential_id) from participant_events where event_offset <= ${endInclusive}"
        .as(get[Option[Long]](1).single)(connection)
        .getOrElse(
          if (isLedgerEmpty(connection))
            EmptyEventSeqIdRange.endInclusive
          else
            throw new IllegalStateException(
              s"readUpperBound($endInclusive) returned None on non-empty ledger")
        )
    }

  private def isLedgerEmpty(connection: java.sql.Connection): Boolean = {
    SQL"select count(1) = 0 where exists (select * from participant_events)"
      .as(get[Boolean](1).single)(connection)
  }
}
