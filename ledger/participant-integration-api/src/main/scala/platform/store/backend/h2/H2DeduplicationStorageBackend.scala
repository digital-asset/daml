// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection

import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.DeduplicationStorageBackendTemplate

import scala.util.control.NonFatal

object H2DeduplicationStorageBackend extends DeduplicationStorageBackendTemplate {
  private val logger = ContextualizedLogger.get(this.getClass)

  override def upsertDeduplicationEntry(
      key: String,
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int = {

    // Under the default READ_COMMITTED isolation level used for the indexdb, when a deduplication
    // upsert is performed simultaneously from multiple threads, the query fails with
    // JdbcSQLIntegrityConstraintViolationException: Unique index or primary key violation
    // Simple retry helps
    def retry[T](op: => T): T =
      try {
        op
      } catch {
        case NonFatal(e) =>
          logger.debug(s"Caught exception while upserting a deduplication entry: $e")
          op
      }
    retry(
      SQL"""
        merge into participant_command_submissions pcs
        using dual on deduplication_key = ${key}
        when not matched then
          insert (deduplication_key, deduplicate_until)
          values (${key}, ${deduplicateUntil.micros})
        when matched and pcs.deduplicate_until < ${submittedAt.micros} then
          update set deduplicate_until=${deduplicateUntil.micros}
      """
        .executeUpdate()(connection)
    )
  }
}
