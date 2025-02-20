// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend

import java.sql.Connection

trait QueryValidRange {
  def withRangeNotPruned[T](
      minOffsetInclusive: Offset,
      maxOffsetInclusive: Offset,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => T)(implicit
      conn: Connection,
      loggingContext: LoggingContextWithTrace,
  ): T

  def withOffsetNotBeforePruning[T](
      offset: Offset,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => T)(implicit
      conn: Connection,
      loggingContext: LoggingContextWithTrace,
  ): T

}

final case class QueryValidRangeImpl(
    storageBackend: ParameterStorageBackend,
    val loggerFactory: NamedLoggerFactory,
) extends QueryValidRange
    with NamedLogging {

  /** Runs a query and throws an error if the query accesses an invalid offset range.
    *
    * @param query
    *   query to execute
    * @param minOffsetInclusive
    *   minimum, inclusive offset used by the query (i.e. all fetched offsets are larger or equal)
    * @param maxOffsetInclusive
    *   maximum, inclusive offset used by the query (i.e. all fetched offsets are before or equal)
    * @param errorPruning
    *   function that generates a context-specific error parameterized by participant pruning offset
    * @param errorLedgerEnd
    *   function that generates a context-specific error parameterized by ledger end offset
    * @tparam T
    *   type of result passed through
    * @return
    *   either an Error if offset range violates conditions or query result
    *
    * Note in order to prevent race condition on connections at READ_COMMITTED isolation levels (in
    * fact any level below SNAPSHOT isolation level), this check must be performed after fetching
    * the corresponding range of data. This way we avoid a race between pruning and the query
    * reading the offsets in which offsets are "silently skipped". First fetching the objects and
    * only afterwards checking that no pruning operation has interfered, avoids such a race
    * condition.
    */
  override def withRangeNotPruned[T](
      minOffsetInclusive: Offset,
      maxOffsetInclusive: Offset,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => T)(implicit
      conn: Connection,
      loggingContext: LoggingContextWithTrace,
  ): T = {
    assert(Option(maxOffsetInclusive) >= minOffsetInclusive.decrement)
    val result = query
    val params = storageBackend.prunedUpToInclusiveAndLedgerEnd(conn)

    params.pruneUptoInclusive
      .filter(_ >= minOffsetInclusive)
      .foreach(pruningOffsetUpToInclusive =>
        throw RequestValidationErrors.ParticipantPrunedDataAccessed
          .Reject(
            cause = errorPruning(pruningOffsetUpToInclusive),
            earliestOffset = pruningOffsetUpToInclusive.unwrap,
          )(
            ErrorLoggingContext(logger, loggingContext)
          )
          .asGrpcError
      )

    if (Option(maxOffsetInclusive) > params.ledgerEnd) {
      throw RequestValidationErrors.ParticipantDataAccessedAfterLedgerEnd
        .Reject(
          cause = errorLedgerEnd(params.ledgerEnd),
          latestOffset = params.ledgerEnd.fold(0L)(_.unwrap),
        )(
          ErrorLoggingContext(logger, loggingContext)
        )
        .asGrpcError
    }

    result
  }

  override def withOffsetNotBeforePruning[T](
      offset: Offset,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => T)(implicit
      conn: Connection,
      loggingContext: LoggingContextWithTrace,
  ): T =
    withRangeNotPruned(
      // as the range not pruned forms a condition that the minOffsetInclusive is greater than the pruning offset,
      // by setting this to the offset + 1 we ensure that the offset is greater than or equal to the pruning offset.
      minOffsetInclusive = offset.increment,
      maxOffsetInclusive = offset,
      errorPruning = errorPruning,
      errorLedgerEnd = errorLedgerEnd,
    )(query)
}
