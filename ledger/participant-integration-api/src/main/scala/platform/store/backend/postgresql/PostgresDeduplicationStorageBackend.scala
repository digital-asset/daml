// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.common.DeduplicationStorageBackendTemplate

object PostgresDeduplicationStorageBackend extends DeduplicationStorageBackendTemplate {

  override def upsertDeduplicationEntry(
      key: String,
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int =
    SQL"""
      insert into participant_command_submissions as pcs (deduplication_key, deduplicate_until)
      values ($key, ${deduplicateUntil.micros})
      on conflict (deduplication_key)
        do update
        set deduplicate_until=${deduplicateUntil.micros}
        where pcs.deduplicate_until < ${submittedAt.micros}
    """
      .executeUpdate()(connection)
}
