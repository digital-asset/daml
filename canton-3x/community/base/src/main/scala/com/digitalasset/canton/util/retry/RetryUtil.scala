// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.resource.DatabaseStorageError.DatabaseStorageDegradation.DatabaseTaskRejected
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil.ForFailedOps
import org.postgresql.util.PSQLException

import java.sql.*
import scala.annotation.tailrec
import scala.util.{Failure, Try}

object RetryUtil {

  /** When using retry code in different contexts, different exceptions should be retried on. This trait provides a
    * way to define what exceptions should be retried and which are fatal.
    */
  trait ExceptionRetryable {

    /** Determines what kind of error (if any) resulted in the outcome,
      * and gives a recommendation on how many times to retry.
      *
      * Also logs the embedded exception.
      */
    def retryOK(outcome: Try[_], logger: TracedLogger, lastErrorKind: Option[ErrorKind])(implicit
        tc: TraceContext
    ): ErrorKind

    protected def logThrowable(e: Throwable, logger: TracedLogger)(implicit
        traceContext: TraceContext
    ): Unit = e match {
      case sqlE: SQLException =>
        // Unfortunately, the sql state and error code won't get logged automatically.
        logger.info(
          s"Detected an SQLException. SQL state: ${sqlE.getSQLState}, error code: ${sqlE.getErrorCode}",
          e,
        )
      case _: Throwable =>
        logger.info(s"Detected an error.", e)
    }
  }

  sealed trait ErrorKind {
    def maxRetries: Int
  }

  case object NoErrorKind extends ErrorKind {
    override val maxRetries: Int = Int.MaxValue

    override def toString: String = "no error (request infinite retries)"
  }

  case object FatalErrorKind extends ErrorKind {
    override val maxRetries = 0

    override def toString: String = "fatal error (give up immediately)"
  }

  /** Main use case is a network outage. Infinite retries are needed, as we don't know how long the outage takes.
    */
  case object TransientErrorKind extends ErrorKind {
    override val maxRetries: Int = Int.MaxValue

    override def toString: String = "transient error (request infinite retries)"
  }

  /** Main use case is a transient unique constraint violation due to racy merge statements.
    * Should go away after a very limited amount of retries.
    */
  case object SpuriousTransientErrorKind extends ErrorKind {
    // Value determined empirically in UpsertTestOracle.
    // For single row inserts, 1 is sufficient.
    // For batched inserts, 3 was more than sufficient in the test.
    override val maxRetries = 10

    override def toString: String =
      s"possibly spurious transient error (request up to $maxRetries retries)"
  }

  /** Defines which should be retryable when thrown by the database.
    */
  case object DbExceptionRetryable extends ExceptionRetryable {

    def retryOKForever(error: Throwable, logger: TracedLogger)(implicit
        tc: TraceContext
    ): Boolean = {
      // Don't retry forever on "contention" errors, as these may not actually be due to contention and get stuck
      // forever. Eg unique constraint violation exceptions can be caused by contention in H2 leading to data anomalies.
      DbExceptionRetryable.retryOK(Failure(error), logger, None).maxRetries == Int.MaxValue
    }

    override def retryOK(
        outcome: Try[_],
        logger: TracedLogger,
        lastErrorKind: Option[ErrorKind],
    )(implicit
        tc: TraceContext
    ): ErrorKind = {
      outcome match {
        case util.Success(_) => NoErrorKind
        case ff @ Failure(exception) =>
          val errorKind = retryOKInternal(ff, logger)
          // only log the full exception if the error kind changed such that we avoid spamming the logs
          if (!lastErrorKind.contains(errorKind)) {
            logThrowable(exception, logger)
          } else {
            logger.debug(
              s"Retrying on same error kind ${errorKind} for ${exception.getClass.getSimpleName}/${exception.getMessage}"
            )
          }
          errorKind
      }
    }

    private def retryOKInternal(
        outcome: Failure[_],
        logger: TracedLogger,
    )(implicit
        tc: TraceContext
    ): ErrorKind = {
      outcome.exception match {
        case exn: java.util.concurrent.RejectedExecutionException =>
          // This occurs when slick's task queue is full

          // Create a CantonError so that the error code gets logged.
          DatabaseTaskRejected(exn.toString)(
            ErrorLoggingContext.fromTracedLogger(logger)
          ).discard

          TransientErrorKind
        case other => determineErrorKind(other, logger)
      }
    }

    @tailrec def determineErrorKind(
        exception: Throwable,
        logger: TracedLogger,
    )(implicit
        tc: TraceContext
    ): ErrorKind = exception match {
      case exception: PSQLException =>
        // Error codes documented here: https://www.postgresql.org/docs/9.6/errcodes-appendix.html
        val error = exception.getSQLState

        if (error.startsWith("08")) {
          // Class 08 — Connection Exception
          TransientErrorKind
        } else if (error == "40001") {
          // Class 40 — Transaction Rollback: 40001	serialization_failure
          // Failure to serialize db accesses, happens due to contention
          TransientErrorKind
        } else if (error == "40P01") {
          // Deadlock
          // See DatabaseDeadlockTestPostgres
          // This also covers deadlocks reported as BatchUpdateExceptions,
          // because they refer to a PSQLException has cause.
          TransientErrorKind
        } else if (error == "25006") {
          // Retry on read only transaction, which can occur on Azure
          TransientErrorKind
        } else if (error.startsWith("57P") && error != "57P014" && error != "57P04") {
          // Retry on operator invention errors, otherwise Canton components crash in an uncontrolled manner when
          // the exception bubbles up (don't retry on `query_canceled` and `database_dropped`)
          TransientErrorKind
        } else if (
          error == "53000" || error == "53100" || error == "53200" || error == "53300" || error == "53400"
        ) {
          // Retry insufficient db resource errors
          TransientErrorKind
        } else {
          // Don't retry on other exceptions. These other exceptions should be those for which retrying typically won't
          // help, for example a unique constraint violation.
          logger.info(s"Fatal sql exception has error code: $error")
          FatalErrorKind
        }

      case _: SQLIntegrityConstraintViolationException =>
        // Both H2 and Oracle may fail with spurious constraint violations, due to racy implementation of the MERGE statements.
        // In H2, this may also occur because it does not properly implement the serializable isolation level.
        // See UpsertTestOracle
        // See https://github.com/h2database/h2database/issues/2167
        SpuriousTransientErrorKind

      case _: SQLRecoverableException | _: SQLTransientException |
          _: SQLNonTransientConnectionException =>
        TransientErrorKind

      // Handle SQLException and all classes that derive from it (e.g. java.sql.BatchUpdateException)
      // Note that if the exception is not known but has a cause, we'll base the retry on the cause
      case ex: SQLException =>
        val code = ex.getErrorCode
        if (ex.getErrorCode == 1) {
          // Retry on ORA-00001: unique constraint violated exception
          SpuriousTransientErrorKind
        } else if (ex.getMessage == "Connection is closed") {
          // May fail with a "Connection is closed" message if the db has gone down
          TransientErrorKind
        } else if (ex.getErrorCode == 4021) {
          // ORA timeout occurred while waiting to lock object
          TransientErrorKind
        } else if (ex.getErrorCode == 54) {
          // ORA timeout occurred while waiting to lock object or because NOWAIT has been set
          // e.g. as part of truncate table
          TransientErrorKind
        } else if (ex.getErrorCode == 60) {
          // Deadlock
          // See DatabaseDeadlockTestOracle
          TransientErrorKind
        } else if (
          ex.getErrorCode == 604 &&
          List("ORA-08176", "ORA-08177").exists(ex.getMessage.contains)
        ) {
          // Oracle failure in a batch operation
          // For Oracle, the `cause` is not always set properly for exceptions. This is a problem for batched queries.
          // So, look through an exception's `message` to see if it contains a retryable problem.
          TransientErrorKind
        } else if (ex.getErrorCode == 8176) {
          // consistent read failure; rollback data not available
          // Cause:  Encountered data changed by an operation that does not generate rollback data
          // Action: In read/write transactions, retry the intended operation.
          TransientErrorKind
        } else if (ex.getErrorCode == 8177) {
          // failure to serialize transaction with serializable isolation level
          TransientErrorKind
        } else if (ex.getErrorCode == 17410) {
          // No more data to read from socket, can be caused by network problems
          SpuriousTransientErrorKind
        } else if (code == 17002) {
          // This has been observed as either IO Error: Connection reset by peer or IO Error: Broken pipe
          // when straight-up killing an Oracle database server (`kill -9 <oracle-pid>`)
          TransientErrorKind
        } else if (code == 1088 || code == 1089 || code == 1090 || code == 1092) {
          // Often observed for orderly Oracle shutdowns
          // https://docs.oracle.com/en/database/oracle/oracle-database/19/errmg/ORA-00910.html#GUID-D9EBDFFA-88C6-4185-BD2C-E1B959A97274
          TransientErrorKind
        } else if (ex.getCause != null) {
          logger.info("Unable to retry on exception, checking cause.")
          determineErrorKind(ex.getCause, logger)
        } else {
          FatalErrorKind
        }

      case _ => FatalErrorKind
    }
  }

  /** Retry on any exception.
    *
    * This is a sensible default choice for non-db tasks with a finite maximum number of retries.
    */
  case object AllExnRetryable extends ExceptionRetryable {

    override def retryOK(outcome: Try[_], logger: TracedLogger, lastErrorKind: Option[ErrorKind])(
        implicit tc: TraceContext
    ): ErrorKind = {
      outcome.forFailed(t => logThrowable(t, logger))
      NoErrorKind
    }

  }

  /** Don't retry on any exception.
    */
  case object NoExnRetryable extends ExceptionRetryable {

    override def retryOK(outcome: Try[_], logger: TracedLogger, lastErrorKind: Option[ErrorKind])(
        implicit tc: TraceContext
    ): ErrorKind = outcome match {
      case Failure(ex) =>
        logThrowable(ex, logger)
        FatalErrorKind
      case util.Success(_) => NoErrorKind
    }
  }
}
