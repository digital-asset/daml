// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.{RowParser, SQL}

import java.sql.Connection
import anorm.SqlParser.long
import anorm.~
import com.daml.platform.store.backend.DebugStorageBackend

private[backend] trait DebugStorageBackendTemplate extends DebugStorageBackend {

  private val allSequentialIds: String =
    s"""
      |SELECT event_sequential_id FROM participant_events_divulgence
      |UNION ALL
      |SELECT event_sequential_id FROM participant_events_create
      |UNION ALL
      |SELECT event_sequential_id FROM participant_events_consuming_exercise
      |UNION ALL
      |SELECT event_sequential_id FROM participant_events_non_consuming_exercise
      |""".stripMargin

  private val SQL_EVENT_SEQUENTIAL_IDS_SUMMARY = SQL(s"""
      |WITH sequential_ids AS ($allSequentialIds)
      |SELECT min(event_sequential_id) as min, max(event_sequential_id) as max, count(event_sequential_id) as count
      |FROM sequential_ids, parameters
      |WHERE event_sequential_id <= parameters.ledger_end_sequential_id
      |""".stripMargin)

  // Don't fetch an unbounded number of rows
  private val maxReportedDuplicates = 100

  private val SQL_DUPLICATE_EVENT_SEQUENTIAL_IDS = SQL(s"""
       |WITH sequential_ids AS ($allSequentialIds)
       |SELECT event_sequential_id as id, count(*) as count
       |FROM sequential_ids, parameters
       |WHERE event_sequential_id <= parameters.ledger_end_sequential_id
       |GROUP BY event_sequential_id
       |HAVING count(*) > 1
       |FETCH NEXT $maxReportedDuplicates ROWS ONLY
       |""".stripMargin)

  case class EventSequentialIdsRow(min: Long, max: Long, count: Long)

  private val eventSequantialIdsParser: RowParser[EventSequentialIdsRow] =
    long("min") ~
      long("max") ~
      long("count") map { case min ~ max ~ count =>
        EventSequentialIdsRow(min, max, count)
      }

  override def verifyIntegrity()(connection: Connection): Unit = {
    val duplicates = SQL_DUPLICATE_EVENT_SEQUENTIAL_IDS
      .as(long("id").*)(connection)
    val summary = SQL_EVENT_SEQUENTIAL_IDS_SUMMARY
      .as(eventSequantialIdsParser.single)(connection)

    // Verify that there are no duplicate ids.
    if (duplicates.nonEmpty) {
      throw new RuntimeException(
        s"Found ${duplicates.length} duplicate event sequential ids. Examples: ${duplicates.mkString(", ")}"
      )
    }

    // Verify that all ids are sequential (i.e., there are no "holes" in the ids).
    // Since we already know that there are not duplicates, it is enough to check that the count is consistent with the range.
    if (summary.count != summary.max - summary.min + 1) {
      throw new RuntimeException(
        s"Event sequential ids are not consecutive. Min=${summary.min}, max=${summary.max}, count=${summary.count}."
      )
    }
  }
}
