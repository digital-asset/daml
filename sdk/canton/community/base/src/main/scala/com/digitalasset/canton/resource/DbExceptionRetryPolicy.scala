// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.resource.DatabaseStorageError.DatabaseStorageDegradation.DatabaseTaskRejected
import com.digitalasset.canton.resource.DbStorage.NoConnectionAvailable
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.ErrorKind.*
import com.digitalasset.canton.util.retry.{ErrorKind, ExceptionRetryPolicy}
import org.postgresql.util.PSQLException
import org.slf4j.event.Level

import java.sql.{
  SQLException,
  SQLIntegrityConstraintViolationException,
  SQLNonTransientConnectionException,
  SQLRecoverableException,
  SQLTransientException,
}
import scala.annotation.tailrec

/** Defines which exceptions should be retryable when thrown by the database. */
object DbExceptionRetryPolicy extends ExceptionRetryPolicy {

  /** Max number of retries for spurious transient errors. Main use case is a transient unique
    * constraint violation due to racy merge statements. Should go away after a very limited amount
    * of retries.
    *
    * Value determined empirically in the now removed UpsertTestOracle. For single row inserts, 1 is
    * sufficient. For batched inserts, 3 was more than sufficient in the test.
    */
  private val spuriousTransientErrorMaxRetries = 10

  @tailrec override def determineExceptionErrorKind(
      exception: Throwable,
      logger: TracedLogger,
  )(implicit
      tc: TraceContext
  ): ErrorKind = exception match {
    case exn: java.util.concurrent.RejectedExecutionException =>
      // This occurs when slick's task queue is full

      // Create a RpcError so that the error code gets logged.
      DatabaseTaskRejected(exn.toString)(
        ErrorLoggingContext.fromTracedLogger(logger)
      ).discard

      TransientErrorKind()
    case exception: PSQLException =>
      // Error codes documented here: https://www.postgresql.org/docs/9.6/errcodes-appendix.html
      val error = exception.getSQLState

      if (error.startsWith("08")) {
        // Class 08 — Connection Exception
        TransientErrorKind()
      } else if (error == "40001") {
        // Class 40 — Transaction Rollback: 40001	serialization_failure
        // Failure to serialize db accesses, happens due to contention
        TransientErrorKind()
      } else if (error == "40P01") {
        // Deadlock
        // See DatabaseDeadlockTestPostgres
        // This also covers deadlocks reported as BatchUpdateExceptions,
        // because they refer to a PSQLException has cause.
        TransientErrorKind()
      } else if (error == "25006") {
        // Retry on read only transaction, which can occur on Azure
        TransientErrorKind()
      } else if (error.startsWith("57P") && error != "57P014" && error != "57P04") {
        // Retry on operator invention errors, otherwise Canton components crash in an uncontrolled manner when
        // the exception bubbles up (don't retry on `query_canceled` and `database_dropped`)
        TransientErrorKind()
      } else if (
        error == "53000" || error == "53100" || error == "53200" || error == "53300" || error == "53400"
      ) {
        // Retry insufficient db resource errors
        TransientErrorKind()
      } else {
        // Don't retry on other exceptions. These other exceptions should be those for which retrying typically won't
        // help, for example a unique constraint violation.
        logger.info(s"Fatal sql exception has error code: $error")
        FatalErrorKind
      }

    case _: SQLIntegrityConstraintViolationException =>
      // H2 may fail with spurious constraint violations, due to racy implementation of the MERGE statements.
      // In H2, this may also occur because it does not properly implement the serializable isolation level.
      // See https://github.com/h2database/h2database/issues/2167
      TransientErrorKind(spuriousTransientErrorMaxRetries)

    case _: SQLRecoverableException | _: SQLTransientException |
        _: SQLNonTransientConnectionException =>
      TransientErrorKind()

    // Handle SQLException and all classes that derive from it (e.g. java.sql.BatchUpdateException)
    // Note that if the exception is not known but has a cause, we'll base the retry on the cause
    case ex: SQLException =>
      if (ex.getMessage == "Connection is closed") {
        // May fail with a "Connection is closed" message if the db has gone down
        TransientErrorKind()
      } else if (ex.getCause != null) {
        logger.info("Unable to retry on exception, checking cause.")
        determineExceptionErrorKind(ex.getCause, logger)
      } else {
        FatalErrorKind
      }

    case _ => FatalErrorKind
  }

  override def retryLogLevel(e: Throwable): Option[Level] = e match {
    case _: NoConnectionAvailable =>
      // Avoid log noise if no connection is available either due to contention or a temporary network problem
      Some(Level.DEBUG)

    case exception: PSQLException =>
      // Error codes documented here: https://www.postgresql.org/docs/9.6/errcodes-appendix.html
      val error = exception.getSQLState
      if (error == "40001") {
        // Class 40 — Transaction Rollback: 40001	serialization_failure
        // Failure to serialize db accesses, happens due to contention
        Some(Level.INFO)
      } else None

    case _ => None
  }
}
