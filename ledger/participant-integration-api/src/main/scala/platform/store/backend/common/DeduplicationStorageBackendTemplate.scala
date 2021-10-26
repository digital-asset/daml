// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import anorm.{RowParser, SQL}
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.store.Conversions.timestampFromMicros
import com.daml.platform.store.backend.DeduplicationStorageBackend

private[backend] trait DeduplicationStorageBackendTemplate extends DeduplicationStorageBackend {

  private val SQL_SELECT_COMMAND = SQL("""
                                         |select deduplicate_until
                                         |from participant_command_submissions
                                         |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private case class ParsedCommandData(deduplicateUntil: Timestamp)

  private val CommandDataParser: RowParser[ParsedCommandData] =
    timestampFromMicros("deduplicate_until")
      .map(ParsedCommandData)

  override def deduplicatedUntil(deduplicationKey: String)(connection: Connection): Timestamp =
    SQL_SELECT_COMMAND
      .on("deduplicationKey" -> deduplicationKey)
      .as(CommandDataParser.single)(connection)
      .deduplicateUntil

  private val SQL_DELETE_EXPIRED_COMMANDS = SQL("""
                                                  |delete from participant_command_submissions
                                                  |where deduplicate_until < {currentTime}
    """.stripMargin)

  override def removeExpiredDeduplicationData(
      currentTime: Timestamp
  )(connection: Connection): Unit = {
    SQL_DELETE_EXPIRED_COMMANDS
      .on("currentTime" -> currentTime.micros)
      .execute()(connection)
    ()
  }

  private val SQL_DELETE_COMMAND = SQL("""
                                         |delete from participant_command_submissions
                                         |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  override def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit = {
    SQL_DELETE_COMMAND
      .on("deduplicationKey" -> deduplicationKey)
      .execute()(connection)
    ()
  }

}
