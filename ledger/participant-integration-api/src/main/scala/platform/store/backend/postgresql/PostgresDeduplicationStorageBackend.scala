// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

import anorm.SQL
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.common.DeduplicationStorageBackendTemplate

object PostgresDeduplicationStorageBackend extends DeduplicationStorageBackendTemplate {

  private val SQL_INSERT_COMMAND: String =
    """insert into participant_command_submissions as pcs (deduplication_key, deduplicate_until)
      |values ({deduplicationKey}, {deduplicateUntil})
      |on conflict (deduplication_key)
      |  do update
      |  set deduplicate_until={deduplicateUntil}
      |  where pcs.deduplicate_until < {submittedAt}""".stripMargin

  override def upsertDeduplicationEntry(
      key: String,
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int =
    SQL(SQL_INSERT_COMMAND)
      .on(
        "deduplicationKey" -> key,
        "submittedAt" -> submittedAt.micros,
        "deduplicateUntil" -> deduplicateUntil.micros,
      )
      .executeUpdate()(connection)
}
