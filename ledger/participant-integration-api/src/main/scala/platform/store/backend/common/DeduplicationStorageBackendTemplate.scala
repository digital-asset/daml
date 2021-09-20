// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant

import anorm.{RowParser, SQL}
import com.daml.platform.store.Conversions.instantFromMicros
import com.daml.platform.store.backend.DeduplicationStorageBackend

private[backend] trait DeduplicationStorageBackendTemplate extends DeduplicationStorageBackend {

  private val SQL_SELECT_COMMAND = SQL("""
                                         |select deduplicate_until
                                         |from participant_command_submissions
                                         |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private case class ParsedCommandData(deduplicateUntil: Instant)

  private val CommandDataParser: RowParser[ParsedCommandData] =
    instantFromMicros("deduplicate_until")
      .map(ParsedCommandData)

  def deduplicatedUntil(deduplicationKey: String)(connection: Connection): Instant =
    SQL_SELECT_COMMAND
      .on("deduplicationKey" -> deduplicationKey)
      .as(CommandDataParser.single)(connection)
      .deduplicateUntil

  private val SQL_DELETE_EXPIRED_COMMANDS = SQL("""
                                                  |delete from participant_command_submissions
                                                  |where deduplicate_until < {currentTime}
    """.stripMargin)

  def removeExpiredDeduplicationData(currentTime: Instant)(connection: Connection): Unit = {
    SQL_DELETE_EXPIRED_COMMANDS
      .on("currentTime" -> Timestamp.instantToMicros(currentTime))
      .execute()(connection)
    ()
  }

  private val SQL_DELETE_COMMAND = SQL("""
                                         |delete from participant_command_submissions
                                         |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit = {
    SQL_DELETE_COMMAND
      .on("deduplicationKey" -> deduplicationKey)
      .execute()(connection)
    ()
  }

}
