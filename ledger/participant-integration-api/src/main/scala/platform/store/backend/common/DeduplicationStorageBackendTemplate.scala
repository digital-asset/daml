// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import anorm.RowParser
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.Conversions.timestampFromMicros
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.DeduplicationStorageBackend

private[backend] trait DeduplicationStorageBackendTemplate extends DeduplicationStorageBackend {
  private case class ParsedCommandData(deduplicateUntil: Timestamp)

  private val CommandDataParser: RowParser[ParsedCommandData] =
    timestampFromMicros("deduplicate_until")
      .map(ParsedCommandData)

  override def deduplicatedUntil(deduplicationKey: String)(connection: Connection): Timestamp =
    SQL"""
      select deduplicate_until
      from participant_command_submissions
      where deduplication_key = ${deduplicationKey}
    """
      .as(CommandDataParser.single)(connection)
      .deduplicateUntil

  override def removeExpiredDeduplicationData(
      currentTime: Timestamp
  )(connection: Connection): Unit = {
    SQL"""
      delete from participant_command_submissions
      where deduplicate_until < ${currentTime.micros}
    """
      .execute()(connection)
    ()
  }

  override def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit = {
    SQL"""
      delete from participant_command_submissions
      where deduplication_key = ${deduplicationKey}
    """
      .execute()(connection)
    ()
  }

}
