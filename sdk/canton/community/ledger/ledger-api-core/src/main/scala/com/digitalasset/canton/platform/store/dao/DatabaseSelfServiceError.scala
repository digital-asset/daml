// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.ledger.error.IndexErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException
import org.postgresql.util.PSQLException

import java.sql.*

/** Wraps SQLExceptions into transient and non-transient errors. Transience classification is done
  * as follows:
  *
  * * Problems that are likely to be resolved with retries are transient errors. For example,
  * network outages or db access serialization problems.
  *
  * * Problems that cannot be recovered from are non-transient. For example, an illegal argument
  * exception inside a database transaction or a unique constraint violation.
  */
object DatabaseSelfServiceError {
  def apply(
      exception: Throwable
  )(implicit errorLoggingContext: ErrorLoggingContext): Throwable = exception match {
    // This frequently occurs when running with H2, because H2 does not properly implement the serializable
    // isolation level. This causes unexpected constraint violation exceptions when running with H2 in the presence
    // of contention. For now, we retry on these exceptions.
    // See https://github.com/h2database/h2database/issues/2167
    case ex: JdbcSQLIntegrityConstraintViolationException => retryable(ex)
    case ex: SQLRecoverableException => retryable(ex)
    case ex: SQLTransientException => retryable(ex)
    case ex: SQLNonTransientException => nonRetryable(ex)
    case ex: PSQLException => if (isRetryablePsqlException(ex)) retryable(ex) else nonRetryable(ex)
    case ex: BatchUpdateException if ex.getCause != null => DatabaseSelfServiceError(ex.getCause)
    case ex: SQLException => nonRetryable(ex)
    // Don't handle other exceptions that can be thrown from non-client interactions (e.g. index initialization)
    case ex => ex
  }

  private def retryable(ex: SQLException)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): StatusRuntimeException =
    IndexErrors.DatabaseErrors.SqlTransientError.Reject(ex).asGrpcError

  private def nonRetryable(ex: SQLException)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): StatusRuntimeException =
    IndexErrors.DatabaseErrors.SqlNonTransientError.Reject(ex).asGrpcError

  // PostgreSQL exceptions are handled based on the SQLState
  // https://www.postgresql.org/docs/11/errcodes-appendix.html
  private def isRetryablePsqlException(exception: PSQLException): Boolean =
    exception.getSQLState match {
      // Class 08 — Connection Exception
      case state if state.startsWith("08") => true
      // Failure to serialize db accesses, happens due to contention
      case "40001" => true
      // Retry on read only transaction, which can occur on Azure
      case "25006" => true
      // Retry on operator intervention errors, but not on `query_canceled` and `database_dropped`
      case state if state.startsWith("57P") && state != "57014" && state != "57P04" => true
      case _ => false
    }
}
