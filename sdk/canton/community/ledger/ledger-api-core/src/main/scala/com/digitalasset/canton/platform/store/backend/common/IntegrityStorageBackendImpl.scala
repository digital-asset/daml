// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{byteArray, int, long, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.IntegrityStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.google.common.annotations.VisibleForTesting

import java.sql.Connection

private[backend] object IntegrityStorageBackendImpl extends IntegrityStorageBackend {
  import com.digitalasset.canton.platform.store.backend.Conversions.*

  private val allSequentialIds: String =
    s"""
      |SELECT event_sequential_id FROM lapi_events_activate_contract
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_deactivate_contract
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_various_witnessed
      |UNION ALL
      |SELECT event_sequential_id FROM lapi_events_party_to_participant
      |""".stripMargin

  private val allSequentialIdsAndOffsets: String =
    s"""
       |SELECT event_sequential_id, event_offset FROM lapi_events_activate_contract
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_deactivate_contract
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_various_witnessed
       |UNION ALL
       |SELECT event_sequential_id, event_offset FROM lapi_events_party_to_participant
       |""".stripMargin

  private val SqlEventSequentialIdsSummary = SQL"""
      WITH sequential_ids AS (#$allSequentialIdsAndOffsets)
      SELECT min(event_sequential_id) as min, max(event_sequential_id) as max, count(event_sequential_id) as count
      FROM sequential_ids, lapi_parameters
      WHERE
        lapi_parameters.ledger_end_sequential_id is not null and
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
       WHERE lapi_parameters.ledger_end_sequential_id is not null
       AND event_sequential_id <= lapi_parameters.ledger_end_sequential_id
       GROUP BY event_sequential_id
       HAVING count(*) > 1
       FETCH NEXT #$maxReportedDuplicates ROWS ONLY
       """

  private val allEventIds: String =
    s"""
       |SELECT event_offset, node_id FROM lapi_events_activate_contract
       |UNION ALL
       |SELECT event_offset, node_id FROM lapi_events_deactivate_contract
       |UNION ALL
       |SELECT event_offset, node_id FROM lapi_events_various_witnessed
       |""".stripMargin

  private val SqlDuplicateOffsets = SQL"""
       WITH event_ids AS (#$allEventIds)
       SELECT event_offset, node_id, count(*) as count
       FROM event_ids, lapi_parameters
       WHERE lapi_parameters.ledger_end is not null
       AND event_offset <= lapi_parameters.ledger_end
       GROUP BY event_offset, node_id
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

  @VisibleForTesting
  override def verifyIntegrity(
      failForEmptyDB: Boolean,
      inMemoryCantonStore: Boolean,
  )(connection: Connection): Unit = try {
    val duplicateSeqIds = SqlDuplicateEventSequentialIds
      .as(long("id").*)(connection)
    val duplicateOffsets = SqlDuplicateOffsets
      .as(long("event_offset").*)(connection)
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

    // Verify monotonic record times per synchronizer
    val offsetSynchronizerRecordTime = SQL"""
       SELECT event_offset as _offset, record_time, synchronizer_id FROM lapi_events_activate_contract
       UNION ALL
       SELECT event_offset as _offset, record_time, synchronizer_id FROM lapi_events_deactivate_contract
       UNION ALL
       SELECT event_offset as _offset, record_time, synchronizer_id FROM lapi_events_various_witnessed
       UNION ALL
       SELECT completion_offset as _offset, record_time, synchronizer_id FROM lapi_command_completions
       UNION ALL
       SELECT event_offset as _offset, record_time, synchronizer_id FROM lapi_events_party_to_participant
       """.asVectorOf(
      offset("_offset") ~ long("record_time") ~ int("synchronizer_id") map {
        case offset ~ recordTimeMicros ~ internedSynchronizerId =>
          (offset.unwrap, internedSynchronizerId, recordTimeMicros)
      }
    )(connection)
    offsetSynchronizerRecordTime.groupBy(_._2).foreach {
      case (_, offsetRecordTimePerSynchronizer) =>
        val inOrderElems = offsetRecordTimePerSynchronizer.sortBy(_._1)
        inOrderElems.iterator.zip(inOrderElems.iterator.drop(1)).foreach {
          case ((firstOffset, _, firstRecordTime), (secondOffset, _, secondRecordTime)) =>
            if (firstRecordTime > secondRecordTime) {
              throw new RuntimeException(
                s"occurrence of decreasing record time found within one synchronizer: offsets ${Offset
                    .tryFromLong(firstOffset)},${Offset.tryFromLong(secondOffset)} record times: ${CantonTimestamp
                    .assertFromLong(firstRecordTime)},${CantonTimestamp.assertFromLong(secondRecordTime)}"
              )
            }
        }
    }

    // Verify no duplicate update id
    SQL"""
          SELECT meta1.update_id as uId, meta1.event_offset as offset1, meta2.event_offset as offset2
          FROM lapi_update_meta as meta1, lapi_update_meta as meta2
          WHERE meta1.update_id = meta2.update_id and
                meta1.event_offset != meta2.event_offset
          FETCH NEXT 1 ROWS ONLY
      """
      .asSingleOpt(updateId("uId") ~ offset("offset1") ~ offset("offset2"))(connection)
      .foreach { case uId ~ offset1 ~ offset2 =>
        throw new RuntimeException(
          s"occurrence of duplicate update ID [${uId.toHexString}] found for offsets $offset1, $offset2"
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
           SELECT event_offset as _offset, publication_time FROM lapi_update_meta
           UNION ALL
           SELECT completion_offset as _offset, publication_time FROM lapi_command_completions
           """
        .asVectorOf(
          offset("_offset") ~ long("publication_time") map { case offset ~ publicationTime =>
            (offset.unwrap, publicationTime)
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
    val completions = SQL"""
          SELECT
            completion_offset,
            user_id,
            submitters,
            command_id,
            update_id,
            submission_id,
            message_uuid,
            record_time,
            synchronizer_id
          FROM lapi_command_completions
      """
      .asVectorOf(
        offset("completion_offset") ~
          int("user_id") ~
          byteArray("submitters") ~
          str("command_id") ~
          updateId("update_id").? ~
          str("submission_id").? ~
          str("message_uuid").? ~
          long("record_time") ~
          long("synchronizer_id") map {
            case offset ~ userId ~ submitters ~ commandId ~ updateId ~ submissionId ~ messageUuid ~ recordTimeLong ~ synchronizerId =>
              CompletionEntry(
                userId,
                IntArrayDBSerialization.decodeFromByteArray(submitters).toList,
                commandId,
                updateId,
                submissionId,
                messageUuid,
                recordTimeLong,
                synchronizerId,
              ) -> offset
          }
      )(connection)

    // duplicate completions by many fields
    completions
      .groupMapReduce(_._1)(entry => List(entry._2))(_ ::: _)
      .find(_._2.sizeIs > 1)
      .map(_._2)
      .foreach(offsets =>
        throw new RuntimeException(
          s"duplicate entries found in lapi_command_completions at offsets (first 10 shown) ${offsets.take(10)}"
        )
      )

    // duplicate completions by messageUuid
    completions
      .map { case (entry, offset) =>
        (entry.messageUuid, offset)
      }
      .collect { case (Some(messageUuid), offset) =>
        (messageUuid, offset)
      }
      .groupMapReduce(_._1)(entry => List(entry._2))(_ ::: _)
      .find(_._2.sizeIs > 1)
      .map(_._2)
      .foreach(offsets =>
        throw new RuntimeException(
          s"duplicate entries found by messageUuid in lapi_command_completions at offsets (first 10 shown) ${offsets
              .take(10)}"
        )
      )

    // Verify no duplicate completion entry
    val internalContractIds = SQL"""
          SELECT
            internal_contract_id
          FROM par_contracts
      """
      .asVectorOf(long("internal_contract_id"))(connection)
    val firstTenDuplicatedInternalIds = internalContractIds
      .groupMap(identity)(identity)
      .iterator
      .filter(_._2.sizeIs > 1)
      .take(10)
      .map(_._1)
      .toSeq
    if (firstTenDuplicatedInternalIds.nonEmpty)
      throw new RuntimeException(
        s"duplicate internal_contract_id-s found in table par_contracts (first 10 shown) $firstTenDuplicatedInternalIds"
      )

    val pruning_started_up_to_inclusive =
      SQL"""SELECT started_up_to_inclusive FROM par_pruning_operation
          """
        .asSingleOpt(long("started_up_to_inclusive"))(connection)
        .getOrElse(-1L)

    if (!inMemoryCantonStore) {
      val referencedInternalContractIdsWithOffset = SQL"""
              SELECT internal_contract_id, event_offset
              FROM lapi_events_activate_contract
              WHERE event_offset > $pruning_started_up_to_inclusive
              UNION ALL
              SELECT internal_contract_id, event_offset -- activations which were not deactivated before pruning point
              FROM lapi_events_activate_contract
              WHERE event_offset <= $pruning_started_up_to_inclusive
              AND NOT EXISTS (
                SELECT 1
                FROM lapi_events_deactivate_contract
                WHERE lapi_events_deactivate_contract.deactivated_event_sequential_id = lapi_events_activate_contract.event_sequential_id
                AND lapi_events_deactivate_contract.event_offset <= $pruning_started_up_to_inclusive
              )
              UNION ALL
              SELECT internal_contract_id, event_offset
              FROM lapi_events_deactivate_contract
              WHERE event_offset > $pruning_started_up_to_inclusive
                AND internal_contract_id IS NOT NULL
              UNION ALL
              SELECT internal_contract_id, event_offset
              FROM lapi_events_various_witnessed
              WHERE event_offset > $pruning_started_up_to_inclusive
                AND internal_contract_id IS NOT NULL
          """
        .asVectorOf(long("internal_contract_id") ~ long("event_offset") map {
          case internal_contract_id ~ event_offset => (internal_contract_id, event_offset)
        })(connection)
      val strayInternalContractIdsWithOffset =
        referencedInternalContractIdsWithOffset
          .filterNot(p => internalContractIds.contains(p._1))
          .distinct
          .sorted
      if (strayInternalContractIdsWithOffset.nonEmpty) {
        throw new RuntimeException(
          s"some internal_contract_id-s in events tables are not present in par_contracts (first 10 shown with offsets) ${strayInternalContractIdsWithOffset
              .take(10)
              .mkString("[", ", ", "]")}"
        )
      }
    }

    val strayDeactivationsWithOffset =
      SQL"""
        SELECT deactivated_event_sequential_id, event_offset
        FROM lapi_events_deactivate_contract dea, lapi_parameters
        WHERE deactivated_event_sequential_id IS NOT NULL AND NOT EXISTS (
          SELECT 1
          FROM lapi_events_activate_contract act
          WHERE act.event_sequential_id < dea.event_sequential_id
          AND act.event_sequential_id = dea.deactivated_event_sequential_id
        )
        AND lapi_parameters.ledger_end is not null
        AND event_offset <= lapi_parameters.ledger_end
    """
        .asVectorOf(
          long("lapi_events_deactivate_contract.deactivated_event_sequential_id") ~ long(
            "event_offset"
          ) map { case deactivated_event_sequential_id ~ event_offset =>
            (deactivated_event_sequential_id, event_offset)
          }
        )(
          connection
        )
        .sorted
    if (strayDeactivationsWithOffset.nonEmpty)
      throw new RuntimeException(
        s"some deactivation events do not have a preceding activation event, deactivated_event_sequential_id-s with offsets (first 10 shown) ${strayDeactivationsWithOffset
            .take(10)
            .mkString("[", ", ", "]")}"
      )

    val eventSequentialIdsWithMissingMandatories =
      SQL"""
          SELECT event_sequential_id, event_offset
          FROM lapi_events_activate_contract
          WHERE (event_type = 2 -- assign
            AND (source_synchronizer_id IS NULL OR reassignment_counter IS NULL OR reassignment_id IS NULL))

          UNION ALL

          SELECT event_sequential_id, event_offset
          FROM lapi_events_deactivate_contract
          WHERE (event_type = 3 -- consuming exercise
            AND (
              additional_witnesses IS NULL OR
              exercise_choice IS NULL OR
              exercise_argument IS NULL OR
              exercise_result IS NULL OR
              exercise_actors IS NULL OR
              contract_id IS NULL OR
              ledger_effective_time IS NULL
            )
          ) OR (event_type = 4 -- unassign
            AND (
              reassignment_id IS NULL OR
              target_synchronizer_id IS NULL OR
              reassignment_counter IS NULL OR
              contract_id IS NULL
            )
          )

          UNION ALL

          SELECT event_sequential_id, event_offset
          FROM lapi_events_various_witnessed
          WHERE (event_type = 5 -- non-consuming exercise
            AND (
              consuming IS NULL OR
              exercise_choice IS NULL OR
              exercise_argument IS NULL OR
              exercise_result IS NULL OR
              exercise_actors IS NULL OR
              contract_id IS NULL OR
              template_id IS NULL OR
              package_id IS NULL OR
              ledger_effective_time IS NULL
            )
          ) OR (event_type = 6 -- witnessed create
            AND (representative_package_id IS NULL OR internal_contract_id IS NULL)
          ) OR (event_type = 7 -- witnessed consuming exercise
            AND (
              consuming IS NULL OR
              exercise_choice IS NULL OR
              exercise_argument IS NULL OR
              exercise_result IS NULL OR
              exercise_actors IS NULL OR
              contract_id IS NULL OR
              template_id IS NULL OR
              package_id IS NULL OR
              ledger_effective_time IS NULL
            )
          )
      """
        .asVectorOf(long("event_sequential_id") ~ long("event_offset") map {
          case event_sequential_id ~ event_offset => (event_sequential_id, event_offset)
        })(connection)
    if (eventSequentialIdsWithMissingMandatories.nonEmpty)
      throw new RuntimeException(
        s"some events are missing mandatory fields, event_sequential_ids, offsets (first 10 shown) ${eventSequentialIdsWithMissingMandatories
            .take(10)
            .mkString("[", ", ", "]")}"
      )
  } catch {
    case t: Throwable if !failForEmptyDB =>
      val failure = t.getMessage
      val postgresEmptyDBError = failure.contains(
        "relation \"lapi_events_activate_contract\" does not exist"
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

  @VisibleForTesting
  override def numberOfAcceptedTransactionsFor(
      synchronizerId: SynchronizerId
  )(connection: Connection): Int =
    SQL"""SELECT internal_id
          FROM lapi_string_interning
          WHERE external_string = ${"d|" + synchronizerId.toProtoPrimitive}
       """
      .asSingleOpt(int("internal_id"))(connection)
      .map(internedSynchronizerId => SQL"""
        SELECT COUNT(*) as count
        FROM lapi_update_meta
        WHERE synchronizer_id = $internedSynchronizerId
       """.asSingle(int("count"))(connection))
      .getOrElse(0)

  /** ONLY FOR TESTING This is causing wiping of all LAPI event data. This should not be used during
    * working indexer.
    */
  @VisibleForTesting
  override def moveLedgerEndBackToScratch()(connection: Connection): Unit = {
    SQL"UPDATE lapi_parameters SET ledger_end = 1, ledger_end_sequential_id = 0"
      .executeUpdate()(connection)
      .discard
    SQL"DELETE FROM lapi_post_processing_end".executeUpdate()(connection).discard
    SQL"DELETE FROM lapi_ledger_end_synchronizer_index".executeUpdate()(connection).discard
    SQL"DELETE FROM par_command_deduplication".executeUpdate()(connection).discard
    SQL"DELETE FROM par_in_flight_submission".executeUpdate()(connection).discard
  }

  private final case class CompletionEntry(
      userId: Int,
      submitters: List[Int],
      commandId: String,
      updateId: Option[UpdateId],
      submissionId: Option[String],
      messageUuid: Option[String],
      recordTimeLong: Long,
      synchronizerId: Long,
  )
}
