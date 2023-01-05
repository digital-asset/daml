// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import anorm.RowParser

import java.sql.Connection
import anorm.SqlParser.{long, str}
import anorm.~
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.IntegrityStorageBackend

private[backend] object IntegrityStorageBackendImpl extends IntegrityStorageBackend {

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

  private val SqlEventSequentialIdsSummary = SQL"""
      WITH sequential_ids AS (#$allSequentialIds)
      SELECT min(event_sequential_id) as min, max(event_sequential_id) as max, count(event_sequential_id) as count
      FROM sequential_ids, parameters
      WHERE event_sequential_id <= parameters.ledger_end_sequential_id
      """

  // Don't fetch an unbounded number of rows
  private val maxReportedDuplicates = 100

  private val SqlDuplicateEventSequentialIds = SQL"""
       WITH sequential_ids AS (#$allSequentialIds)
       SELECT event_sequential_id as id, count(*) as count
       FROM sequential_ids, parameters
       WHERE event_sequential_id <= parameters.ledger_end_sequential_id
       GROUP BY event_sequential_id
       HAVING count(*) > 1
       FETCH NEXT #$maxReportedDuplicates ROWS ONLY
       """

  private val allEventIds: String =
    s"""
       |SELECT event_offset, node_index FROM participant_events_create
       |UNION ALL
       |SELECT event_offset, node_index FROM participant_events_consuming_exercise
       |UNION ALL
       |SELECT event_offset, node_index FROM participant_events_non_consuming_exercise
       |""".stripMargin

  private val SqlDuplicateOffsets = SQL"""
       WITH event_ids AS (#$allEventIds)
       SELECT event_offset, node_index, count(*) as count
       FROM event_ids, parameters
       WHERE event_offset <= parameters.ledger_end
       GROUP BY event_offset, node_index
       HAVING count(*) > 1
       FETCH NEXT #$maxReportedDuplicates ROWS ONLY
       """

  case class EventSequentialIdsRow(min: Long, max: Long, count: Long)

  private val eventSequantialIdsParser: RowParser[EventSequentialIdsRow] =
    long("min") ~
      long("max") ~
      long("count") map { case min ~ max ~ count =>
        EventSequentialIdsRow(min, max, count)
      }

  override def verifyIntegrity()(connection: Connection): Unit = {
    val duplicateSeqIds = SqlDuplicateEventSequentialIds
      .as(long("id").*)(connection)
    val duplicateOffsets = SqlDuplicateOffsets
      .as(str("event_offset").*)(connection)
    val summary = SqlEventSequentialIdsSummary
      .as(eventSequantialIdsParser.single)(connection)

    // Verify that there are no duplicate offsets (events with the same offset and node index).
    if (duplicateOffsets.nonEmpty) {
      throw new RuntimeException(
        s"Found ${duplicateOffsets.length} duplicate offsets. Examples: ${duplicateOffsets.mkString(", ")}"
      )
    }

    // Verify that there are no duplicate event sequential ids.
    if (duplicateSeqIds.nonEmpty) {
      throw new RuntimeException(
        s"Found ${duplicateSeqIds.length} duplicate event sequential ids. Examples: ${duplicateSeqIds
            .mkString(", ")}"
      )
    }

    // Verify that all event sequential ids are in fact sequential (i.e., there are no "holes" in the ids).
    // Since we already know that there are no duplicates, it is enough to check that the count is consistent with the range.
    if (summary.count != summary.max - summary.min + 1) {
      throw new RuntimeException(
        s"Event sequential ids are not consecutive. Min=${summary.min}, max=${summary.max}, count=${summary.count}."
      )
    }
  }
}
