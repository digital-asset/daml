// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.error.ContextualizedErrorLogger
import com.daml.platform.error.definitions.IndexErrors
import io.grpc.StatusRuntimeException
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException
import org.postgresql.util.PSQLException

import java.sql._
import scala.annotation.tailrec

/** Wraps SQLExceptions into transient and non-transient errors.
  * Transience classification is done as follows:
  *
  * * Problems that are likely to be resolved with retries are transient errors. For example, network outages or db access
  * serialization problems.
  *
  * * Problems that cannot be recovered from are non-transient. For example, an illegal argument exception inside a
  * database transaction or a unique constraint violation.
  */
object DatabaseSelfServiceError {
  @tailrec
  def apply(
      exception: Throwable
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Throwable = exception match {
    // This frequently occurs when running with H2, because H2 does not properly implement the serializable
    // isolation level. This causes unexpected constraint violation exceptions when running with H2 in the presence
    // of contention. For now, we retry on these exceptions.
    // See https://github.com/h2database/h2database/issues/2167
    case ex: JdbcSQLIntegrityConstraintViolationException => retryable(ex)
    case ex: SQLRecoverableException => retryable(ex)
    case ex: SQLTransientException => retryable(ex)
    case ex: SQLNonTransientException => nonRetryable(ex)
    case ex: PSQLException => if (isRetryablePsqlException(ex)) retryable(ex) else nonRetryable(ex)
    // Oracle uses java.sql.SqlException and java.sql.BatchUpdateException
    case ex: BatchUpdateException if ex.getCause != null => DatabaseSelfServiceError(ex.getCause)
    case ex: SQLException => if (isRetryableOracleException(ex)) retryable(ex) else nonRetryable(ex)
    // Don't handle other exceptions that can be thrown from non-client interactions (e.g. index initialization)
    case ex => ex
  }

  private def retryable(ex: SQLException)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    IndexErrors.DatabaseErrors.SqlTransientError.Reject(ex).asGrpcError

  private def nonRetryable(ex: SQLException)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
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

  // Oracle exceptions are handled based on the error codes
  // https://docs.oracle.com/en/database/oracle/oracle-database/19/errmg/ORA-00000.html#GUID-27437B7F-F0C3-4F1F-9C6E-6780706FB0F6
  private def isRetryableOracleException(ex: SQLException): Boolean =
    ex.getErrorCode match {
      // Unique constraint violated exception
      case 1 => false
      // Timeout occurred while waiting to lock object
      case 54 => true
      // Oracle failure in a batch operation
      case 604 if oracleMessageRetryable(ex) => true
      // consistent read failure; rollback data not available
      case 4021 => true
      // Timeout occurred while waiting to lock object or because NOWAIT has been set
      case 8176 => true
      // Can't serialize access for this transaction
      case 8177 => true
      // Often observed for orderly Oracle shutdowns
      case 1088 | 1089 | 1090 | 1092 => true
      // No more data to read from socket, can be caused by network problems
      case 17002 => true
      // This has been observed as either IO Error: Connection reset by peer or IO Error: Broken pipe
      // when straight-up killing Oracle database server
      case 17410 => true
      case _ if ex.getMessage == "Connection is closed" => true
      case _ => false
    }

  /** For Oracle, the `cause` isn't set properly for exceptions. This is a problem for batched queries.
    * So, look through an exception's `message` to see if it contains a retryable problem.
    */
  private def oracleMessageRetryable(ex: SQLException): Boolean = {
    val consistentReadFailure = "ORA-08176"
    val cantSerializeAccess = "ORA-08177"
    val message = ex.getMessage
    List(consistentReadFailure, cantSerializeAccess).exists(message.contains)
  }
}
