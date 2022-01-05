// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.sql.Connection

import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.DeduplicationStorageBackendTemplate

import scala.util.control.NonFatal

object OracleDeduplicationStorageBackend extends DeduplicationStorageBackendTemplate {
  private val logger = ContextualizedLogger.get(this.getClass)

  override def upsertDeduplicationEntry(
      key: String,
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int = {

    // Under the default READ_COMMITTED isolation level used for the indexdb, when a deduplication
    // upsert is performed simultaneously from multiple threads, the query fails with
    // SQLIntegrityConstraintViolationException: ORA-00001: unique constraint (INDEXDB.SYS_C007590) violated
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
        using dual
        on (pcs.deduplication_key = ${key})
        when matched then
          update set pcs.deduplicate_until=${deduplicateUntil.micros}
          where pcs.deduplicate_until < ${submittedAt.micros}
        when not matched then
          insert (pcs.deduplication_key, pcs.deduplicate_until)
          values (${key}, ${deduplicateUntil.micros})
      """
        .executeUpdate()(connection)
    )
  }

}
