// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error._
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.IndexErrorGroup
import com.daml.error.utils.ErrorDetails
import io.grpc.StatusRuntimeException

@Explanation("Errors raised by the Participant Index persistence layer.")
object IndexErrors extends IndexErrorGroup {
  object DatabaseErrors extends DatabaseErrorGroup {
    @Explanation(
      "This error occurs if a transient error arises when executing a query against the index database."
    )
    @Resolution("Re-submit the request.")
    object SqlTransientError
        extends ErrorCode(
          id = "INDEX_DB_SQL_TRANSIENT_ERROR",
          ErrorCategory.TransientServerFailure,
        )
        with HasUnapply {
      case class Reject(throwable: Throwable)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"Processing the request failed due to a transient database error: ${throwable.getMessage}",
            throwableO = Some(throwable),
          )
    }

    @Explanation(
      "This error occurs if a non-transient error arises when executing a query against the index database."
    )
    @Resolution("Contact the participant operator.")
    object SqlNonTransientError
        extends ErrorCode(
          id = "INDEX_DB_SQL_NON_TRANSIENT_ERROR",
          ErrorCategory.SystemInternalAssumptionViolated,
        )
        with HasUnapply {
      case class Reject(throwable: Throwable)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"Processing the request failed due to a non-transient database error: ${throwable.getMessage}",
            throwableO = Some(throwable),
          )
    }

    @Explanation(
      "This error occurs if the result set returned by a query against the Index database is invalid."
    )
    @Resolution("Contact support.")
    object ResultSetError
        extends ErrorCode(
          id = "INDEX_DB_INVALID_RESULT_SET",
          ErrorCategory.SystemInternalAssumptionViolated,
        )
        with HasUnapply {
      case class Reject(message: String)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause = message
          )
    }
  }

  trait HasUnapply {
    this: ErrorCode =>
    // TODO error codes: Create a generic unapply for ErrorCode that returns the ErrorCode instance
    //                   and match against that one.
    def unapply(exception: StatusRuntimeException): Option[Unit] =
      if (ErrorDetails.isErrorCode(exception)(errorCode = this)) Some(()) else None
  }
}
