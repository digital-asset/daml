// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao.events

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.platform.store.EventSequentialId

// (startExclusive, endInclusive]
private[events] final case class EventsRange[A](startExclusive: A, endInclusive: A) {
  def map[B](f: A => B): EventsRange[B] =
    copy(startExclusive = f(startExclusive), endInclusive = f(endInclusive))
}

private[events] object EventsRange {
  // (0, 0] -- non-existent range
  private val EmptyEventSeqIdRange =
    EventsRange(EventSequentialId.beforeBegin, EventSequentialId.beforeBegin)

  def isEmpty[A: Ordering](range: EventsRange[A]): Boolean = {
    val A = implicitly[Ordering[A]]
    A.gteq(range.startExclusive, range.endInclusive)
  }

  final class EventSeqIdReader(lookupEventSeqId: Offset => Connection => Option[Long]) {

    /** Converts Offset range to Event Sequential ID range.
      *
      * @param range offset range
      * @param connection SQL connection
      * @return Event Sequential ID range
      */
    def readEventSeqIdRange(range: EventsRange[Offset])(
        connection: java.sql.Connection
    ): EventsRange[Long] =
      if (isEmpty(range))
        EmptyEventSeqIdRange
      else
        EventsRange(
          startExclusive = readEventSeqId(range.startExclusive)(connection),
          endInclusive = readEventSeqId(range.endInclusive)(connection),
        )

    /** Converts ledger end offset to Event Sequential ID.
      *
      * @param endInclusive ledger end offset
      * @param connection SQL connection
      * @return Event Sequential ID range.
      */
    def readEventSeqIdRange(endInclusive: Offset)(
        connection: java.sql.Connection
    ): EventsRange[Long] = {
      if (endInclusive == Offset.beforeBegin) EmptyEventSeqIdRange
      else EmptyEventSeqIdRange.copy(endInclusive = readEventSeqId(endInclusive)(connection))
    }

    private def readEventSeqId(offset: Offset)(connection: java.sql.Connection): Long =
      lookupEventSeqId(offset)(connection)
        .getOrElse(EventSequentialId.beforeBegin)
  }

  private[events] def readPage[A](
      read: (
          EventsRange[Long],
          Option[Int],
          Option[Int],
      ) => Connection => Vector[A], // takes range, limit, fetchSize hint
      range: EventsRange[Long],
      pageSize: Int,
  ): Connection => Vector[A] = connection => {
    val minPageSize = 10 min pageSize max (pageSize / 10)
    val guessedPageEnd = range.endInclusive min (range.startExclusive + pageSize)
    val arithPage =
      read(range copy (endInclusive = guessedPageEnd), None, Some(pageSize))(connection)
    val found = arithPage.size
    if (guessedPageEnd == range.endInclusive || found >= minPageSize)
      arithPage
    else
      arithPage ++ read(
        range copy (startExclusive = guessedPageEnd),
        Some(minPageSize - found),
        Some(minPageSize - found),
      )(connection)
  }
}
