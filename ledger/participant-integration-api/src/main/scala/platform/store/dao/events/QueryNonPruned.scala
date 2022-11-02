// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.error.DamlContextualizedErrorLogger

import java.sql.Connection
import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.platform.store.backend.ParameterStorageBackend

trait QueryNonPruned {
  def executeSql[T](
      query: => T,
      minOffsetExclusive: Offset,
      error: Offset => String,
  )(implicit
      conn: Connection,
      loggingContext: LoggingContext,
  ): T
}

case class QueryNonPrunedImpl(
    storageBackend: ParameterStorageBackend
) extends QueryNonPruned {

  private val logger = ContextualizedLogger.get(getClass)

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
      loggingContext: LoggingContext,
  ): T = {
    val result = query

    storageBackend.prunedUpToInclusive(conn) match {
      case None =>
        result

      case Some(pruningOffsetUpToInclusive) if minOffsetExclusive >= pruningOffsetUpToInclusive =>
        result

      case Some(pruningOffsetUpToInclusive) =>
        throw LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed
          .Reject(
            cause = error(pruningOffsetUpToInclusive),
            earliestOffset = pruningOffsetUpToInclusive.toHexString,
          )(
            new DamlContextualizedErrorLogger(logger, loggingContext, None)
          )
          .asGrpcError
    }
  }
}
