// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.StorageErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

object DatabaseStorageError extends StorageErrorGroup {

  @Explanation(
    """This error indicates that degradation of database storage components."""
  )
  @Resolution(
    s"""This error indicates performance degradation. The error occurs when a database task has been rejected,
       |typically due to having a too small task queue. The task will be retried after a delay.
       |If this error occurs frequently, however, you may want to consider increasing the task queue.
       |(Config parameter: canton.<path-to-my-node>.storage.config.queueSize)."""
  )
  object DatabaseStorageDegradation
      extends ErrorCode(
        id = "DB_STORAGE_DEGRADATION",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {

    override protected def exposedViaApi: Boolean = false

    final case class DatabaseTaskRejected(messageFromSlick: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"""A database task was rejected from the database task queue.
           |The full error message from the task queue is:
           |$messageFromSlick""".stripMargin
        ) {}
  }

  @Explanation(
    """This error indicates that the connection to the database has been lost."""
  )
  @Resolution("Inspect error message for details.")
  object DatabaseConnectionLost
      extends ErrorCode(
        id = "DB_CONNECTION_LOST",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {

    override protected def exposedViaApi: Boolean = false

    @Resolution(
      s"""This error indicates that during a database connection health check it was detected that
         | it is not possible to connect to the database. That is, an attempt has been made to connect
         | but it either timed out or failed to check that the connection was valid.""".stripMargin
    )
    final case class DatabaseConnectionLost(messageFromSlick: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Database health check failed to establish a valid connection: $messageFromSlick",
          throwableO = None,
        ) {}
  }
}
