// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import anorm.SQL
import com.daml.ledger.offset.Offset
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.Conversions.offset

object QueryNonPruned {

  /** Runs a query and throws an error if the query accesses pruned offsets.
    *
    * @param query              query to execute
    * @param minOffsetExclusive minimum, exclusive offset used by the query (i.e. all fetched offsets are larger)
    * @param error              function that generates a context-specific error parameterized by participant pruning offset
    * @tparam T type of result passed through
    * @return either a PrunedLedgerOffsetNoLongerAvailable if pruned offset access attempt has occurred or query result
    *
    * Note in order to prevent race condition on connections at READ_COMMITTED isolation levels
    * (in fact any level below SNAPSHOT isolation level), this check must be performed after
    * fetching the corresponding range of data. This way we avoid a race between pruning and
    * the query reading the offsets in which offsets are "silently skipped". First fetching
    * the objects and only afterwards checking that no pruning operation has interfered, avoids
    * such a race condition.
    */
  def executeSql[T](query: => T, minOffsetExclusive: Offset, error: Offset => String)(implicit
      conn: Connection
  ): Either[Throwable, T] = {
    val result = query

    SQL_SELECT_MOST_RECENT_PRUNING
      .as(offset("participant_pruned_up_to_inclusive").?.single)
      .fold(Right(result): Either[Throwable, T])(pruningOffsetUpToInclusive =>
        Either.cond(
          minOffsetExclusive >= pruningOffsetUpToInclusive,
          result,
          ErrorFactories.participantPrunedDataAccessed(error(pruningOffsetUpToInclusive)),
        )
      )
  }

  def executeSqlOrThrow[T](query: => T, minOffsetExclusive: Offset, error: Offset => String)(
      implicit conn: Connection
  ): T =
    executeSql(query, minOffsetExclusive, error).fold(throw _, identity)

  private val SQL_SELECT_MOST_RECENT_PRUNING = SQL(
    "select participant_pruned_up_to_inclusive from parameters"
  )

}
