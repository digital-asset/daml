// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend

import java.sql.Connection

trait QueryNonPruned {
  def executeSql[T](
      query: => T,
      minOffsetExclusive: Offset,
      error: Offset => String,
  )(implicit
      conn: Connection,
      loggingContext: LoggingContextWithTrace,
  ): T
}

final case class QueryNonPrunedImpl(
    storageBackend: ParameterStorageBackend,
    val loggerFactory: NamedLoggerFactory,
) extends QueryNonPruned
    with NamedLogging {

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
  override def executeSql[T](query: => T, minOffsetExclusive: Offset, error: Offset => String)(
      implicit
      conn: Connection,
      loggingContext: LoggingContextWithTrace,
  ): T = {
    val result = query

    storageBackend.prunedUpToInclusive(conn) match {
      case None =>
        result

      case Some(pruningOffsetUpToInclusive) if minOffsetExclusive >= pruningOffsetUpToInclusive =>
        result

      case Some(pruningOffsetUpToInclusive) =>
        throw RequestValidationErrors.ParticipantPrunedDataAccessed
          .Reject(
            cause = error(pruningOffsetUpToInclusive),
            earliestOffset = pruningOffsetUpToInclusive.toHexString,
          )(
            ErrorLoggingContext(logger, loggingContext)
          )
          .asGrpcError
    }
  }
}
