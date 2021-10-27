// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.IndexErrorGroup
import com.daml.error.{ContextualizedErrorLogger, ErrorCategory, ErrorCode, Explanation, Resolution}

import java.sql.{SQLNonTransientException, SQLTransientException}

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
        ) {
      case class Reject(exception: SQLTransientException)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"Processing the request failed due to a transient database error: ${exception.getMessage}",
            throwableO = Some(exception),
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
        ) {
      case class Reject(exception: SQLNonTransientException)(implicit
          val loggingContext: ContextualizedErrorLogger
      ) extends LoggingTransactionErrorImpl(
            cause =
              s"Processing the request failed due to a non-transient database error: ${exception.getMessage}",
            throwableO = Some(exception),
          )
    }
  }
}
