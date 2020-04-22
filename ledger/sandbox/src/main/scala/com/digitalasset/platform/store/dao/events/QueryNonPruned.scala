// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import anorm.SQL
import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.store.Conversions.offset

object QueryNonPruned {

  /**
    * Runs a query and throws an error if the query accesses pruned offsets.
    *
    * @param query    query to execute
    * @param isPruned predicate that checks if we are accessing pruned offsets
    * @param error    parameter-less function that generates a context-specific error
    * @return either a PrunedLedgerOffsetNoLongerAvailable if pruned offset access attempt has occurred or query result
    *
    * Note in order to prevent race condition on connections at READ_COMMITTED isolation levels
    * (in fact any level below SNAPSHOT isolation level), this check must be performed after
    * fetching the corresponding range of data. This way we avoid a race between pruning and
    * the query reading the offsets in which offsets are "silently skipped". First fetching
    * the objects and only afterwards checking that no pruning operation has interfered, avoids
    * such a race condition.
    */
  def executeSql[T](query: => T, isPruned: Offset => Boolean, error: => String)(
      implicit conn: Connection): Either[Throwable, T] = {
    val result = query

    SQL_SELECT_MOST_RECENT_PRUNING
      .as(offset("participant_pruned_up_to_inclusive").?.single)
      .fold(Right(result): Either[Throwable, T])(pruningOffsetUpToInclusive =>
        Either.cond(
          !isPruned(pruningOffsetUpToInclusive),
          result,
          OutOfRangeAccessViolation(
            s"Access violation of pruned data up to ${pruningOffsetUpToInclusive.toHexString}: ${error}")
      ))
  }

  private val SQL_SELECT_MOST_RECENT_PRUNING = SQL(
    "select participant_pruned_up_to_inclusive from parameters"
  )

  final case class OutOfRangeAccessViolation(message: String) extends Throwable

}
