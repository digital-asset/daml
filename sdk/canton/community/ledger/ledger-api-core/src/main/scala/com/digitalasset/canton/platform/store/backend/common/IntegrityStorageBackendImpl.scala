// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{array, int, long, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.IntegrityStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.topology.DomainId

import java.sql.Connection

private[backend] object IntegrityStorageBackendImpl extends IntegrityStorageBackend {
  import com.digitalasset.canton.platform.store.backend.Conversions.*

  private val allSequentialIds: String =
    s"""
      |SELECT event_sequential_id FROM lapi_events_create
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_consuming_exercise
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_non_consuming_exercise
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_unassign
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_assign
      |""".stripMargin

  private val allSequentialIdsAndOffsets: String =
    s"""
       |SELECT event_sequential_id, event_offset FROM lapi_events_create
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_consuming_exercise
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_non_consuming_exercise
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_unassign
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_assign
       |""".stripMargin

  private val SqlEventSequentialIdsSummary = SQL"""
      WITH sequential_ids AS (#$allSequentialIdsAndOffsets)
      SELECT min(event_sequential_id) as min, max(event_sequential_id) as max, count(event_sequential_id) as count
      FROM sequential_ids, lapi_parameters
      WHERE
        event_sequential_id <= lapi_parameters.ledger_end_sequential_id and
        (
          lapi_parameters.participant_pruned_up_to_inclusive is null or
          event_offset > lapi_parameters.participant_pruned_up_to_inclusive -- this is not backed up by index, but it is okay as this is for testing
        )
      """

  // Don't fetch an unbounded number of rows
  private val maxReportedDuplicates = 100

  private val SqlDuplicateEventSequentialIds = SQL"""
       WITH sequential_ids AS (#$allSequentialIds)
       SELECT event_sequential_id as id, count(*) as count
       FROM sequential_ids, lapi_parameters
       WHERE event_sequential_id <= lapi_parameters.ledger_end_sequential_id
       GROUP BY event_sequential_id
       HAVING count(*) > 1
       FETCH NEXT #$maxReportedDuplicates ROWS ONLY
       """

  private val allEventIds: String =
    s"""
       |SELECT event_offset, node_index FROM lapi_events_create
       |UNION ALL
       |SELECT event_offset, node_index FROM lapi_events_consuming_exercise
       |UNION ALL
       |SELECT event_offset, node_index FROM lapi_events_non_consuming_exercise
       |UNION ALL
       |SELECT event_offset, 0 FROM lapi_events_unassign
       |UNION ALL
       |SELECT event_offset, 0 FROM lapi_events_assign
       |""".stripMargin

  private val SqlDuplicateOffsets = SQL"""
       WITH event_ids AS (#$allEventIds)
       SELECT event_offset, node_index, count(*) as count
       FROM event_ids, lapi_parameters
       WHERE event_offset <= lapi_parameters.ledger_end
       GROUP BY event_offset, node_index
       HAVING count(*) > 1
       FETCH NEXT #$maxReportedDuplicates ROWS ONLY
       """

  final case class EventSequentialIdsRow(min: Long, max: Long, count: Long)

  private val eventSequantialIdsParser: RowParser[EventSequentialIdsRow] =
    long("min").? ~
      long("max").? ~
      long("count") map { case min ~ max ~ count =>
        EventSequentialIdsRow(min.getOrElse(0L), max.getOrElse(0L), count)
      }

  override def onlyForTestingVerifyIntegrity(
      failForEmptyDB: Boolean = true
  )(connection: Connection): Unit = try {
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
    if (summary.count != 0 && summary.count != summary.max - summary.min + 1) {
      throw new RuntimeException(
        s"Event sequential ids are not consecutive. Min=${summary.min}, max=${summary.max}, count=${summary.count}."
      )
    }

    // Verify monotonic record times per domain
    val offsetDomainRecordTime = SQL"""
       SELECT event_offset as _offset, record_time, domain_id FROM lapi_events_create
       UNION ALL
       SELECT event_offset as _offset, record_time, domain_id FROM lapi_events_consuming_exercise
       UNION ALL
       SELECT event_offset as _offset, record_time, domain_id FROM lapi_events_non_consuming_exercise
       UNION ALL
       SELECT event_offset as _offset, record_time, source_domain_id as domain_id FROM lapi_events_unassign
       UNION ALL
       SELECT event_offset as _offset, record_time, target_domain_id as domain_id FROM lapi_events_assign
       UNION ALL
       SELECT completion_offset as _offset, record_time, domain_id FROM lapi_command_completions
         WHERE message_uuid is null -- it is not a timely reject (the record time there can be a source of violation: in corner cases it can move backwards)
       """.asVectorOf(
      offset("_offset") ~ long("record_time") ~ int("domain_id") map {
        case offset ~ recordTimeMicros ~ internedDomainId =>
          (offset.toLong, internedDomainId, recordTimeMicros)
      }
    )(connection)
    offsetDomainRecordTime.groupBy(_._2).foreach { case (_, offsetRecordTimePerDomain) =>
      val inOrderElems = offsetRecordTimePerDomain.sortBy(_._1)
      inOrderElems.iterator.zip(inOrderElems.iterator.drop(1)).foreach {
        case ((firstOffset, _, firstRecordTime), (secondOffset, _, secondRecordTime)) =>
          if (firstRecordTime > secondRecordTime) {
            throw new RuntimeException(
              s"occurrence of decreasing record time found within one domain: offsets ${Offset
                  .fromLong(firstOffset)},${Offset.fromLong(secondOffset)} record times: ${CantonTimestamp
                  .assertFromLong(firstRecordTime)},${CantonTimestamp.assertFromLong(secondRecordTime)}"
            )
          }
      }
    }

    // Verify no duplicate update id
    SQL"""
          SELECT meta1.transaction_id as txId, meta1.event_offset as offset1, meta2.event_offset as offset2
          FROM lapi_transaction_meta as meta1, lapi_transaction_meta as meta2
          WHERE meta1.transaction_id = meta2.transaction_id and
                meta1.event_offset != meta2.event_offset
          FETCH NEXT 1 ROWS ONLY
      """
      .asSingleOpt(str("txId") ~ offset("offset1") ~ offset("offset2"))(connection)
      .foreach { case txId ~ offset1 ~ offset2 =>
        throw new RuntimeException(
          s"occurrence of duplicate update ID [$txId] found for offsets $offset1, $offset2"
        )
      }

    // Verify no duplicate completion offset
    SQL"""
          SELECT completion_offset, count(*) as offset_count
          FROM lapi_command_completions
          GROUP BY completion_offset
          HAVING count(*) > 1
          FETCH NEXT 1 ROWS ONLY
      """
      .asSingleOpt(offset("completion_offset") ~ int("offset_count"))(connection)
      .foreach { case offset ~ count =>
        throw new RuntimeException(
          s"occurrence of duplicate offset found for lapi_command_completions: for offset $offset $count rows found"
        )
      }

    // Verify publication time cannot go backwards
    val offsetPublicationTimes =
      SQL"""
           SELECT event_offset as _offset, publication_time FROM lapi_transaction_meta
           UNION ALL
           SELECT completion_offset as _offset, publication_time FROM lapi_command_completions
           """
        .asVectorOf(
          offset("_offset") ~ long("publication_time") map { case offset ~ publicationTime =>
            (offset.toLong, publicationTime)
          }
        )(connection)
        .sortBy(_._1)
    offsetPublicationTimes.iterator.zip(offsetPublicationTimes.iterator.drop(1)).foreach {
      case ((offset, publicationTime), (nextOffset, nextPublicationTime)) =>
        if (offset == nextOffset && publicationTime != nextPublicationTime) {
          throw new RuntimeException(
            s"for each offset the publication times should be equal due to indexer batching should respect offset boundaries, but for offset $offset this does not hold"
          )
        }
        if (offset < nextOffset && publicationTime > nextPublicationTime) {
          throw new RuntimeException(
            s"publication_time should monotonic in offset time, but from $offset to $nextOffset publication_time decreased"
          )
        }
    }

    // Verify no duplicate completion entry
    SQL"""
          SELECT
            completion_offset,
            application_id,
            submitters,
            command_id,
            transaction_id,
            submission_id,
            message_uuid,
            request_sequencer_counter,
            domain_id
          FROM lapi_command_completions
      """
      .asVectorOf(
        offset("completion_offset") ~
          str("application_id") ~
          array[Int]("submitters") ~
          str("command_id") ~
          str("transaction_id").? ~
          str("submission_id").? ~
          str("message_uuid").? ~
          long("request_sequencer_counter").? ~
          long("domain_id") map {
            case offset ~ applicationId ~ submitters ~ commandId ~ updateId ~ submissionId ~ messageUuid ~ requestSequencerCounter ~ domainId =>
              (
                applicationId,
                submitters.toList,
                commandId,
                updateId,
                submissionId,
                messageUuid,
                requestSequencerCounter,
                domainId,
              ) -> offset
          }
      )(connection)
      .groupMapReduce(_._1)(entry => List(entry._2))(_ ::: _)
      .find(_._2.size > 1)
      .map(_._2)
      .foreach(offsets =>
        throw new RuntimeException(
          s"duplicate entries found in lapi_command_completions at offsets (first 10 shown) ${offsets.take(10)}"
        )
      )
  } catch {
    case t: Throwable if !failForEmptyDB =>
      val failure = t.getMessage
      val postgresEmptyDBError = failure.contains(
        "relation \"lapi_events_create\" does not exist"
      )
      val h2EmptyDBError = failure.contains(
        "this database is empty"
      )
      if (postgresEmptyDBError || h2EmptyDBError) {
        ()
      } else {
        throw t
      }
  }

  override def onlyForTestingNumberOfAcceptedTransactionsFor(
      domainId: DomainId
  )(connection: Connection): Int =
    SQL"""SELECT internal_id
          FROM lapi_string_interning
          WHERE external_string = ${"d|" + domainId.toProtoPrimitive}
       """
      .asSingleOpt(int("internal_id"))(connection)
      .map(internedDomainId => SQL"""
        SELECT COUNT(*) as count
        FROM lapi_transaction_meta
        WHERE domain_id = $internedDomainId
       """.asSingle(int("count"))(connection))
      .getOrElse(0)

  /**  ONLY FOR TESTING
    *  This is causing wiping of all LAPI event data.
    *  This should not be used during working indexer.
    */
  override def onlyForTestingMoveLedgerEndBackToScratch()(connection: Connection): Unit = {
    SQL"DELETE FROM lapi_parameters".executeUpdate()(connection).discard
    SQL"DELETE FROM lapi_post_processing_end".executeUpdate()(connection).discard
    SQL"DELETE FROM lapi_ledger_end_domain_index".executeUpdate()(connection).discard
    SQL"DELETE FROM lapi_metering_parameters".executeUpdate()(connection).discard
    SQL"DELETE FROM par_command_deduplication".executeUpdate()(connection).discard
    SQL"DELETE FROM par_in_flight_submission".executeUpdate()(connection).discard
  }
}
