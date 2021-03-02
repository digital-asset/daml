package com.daml.platform.store.dao.events

import java.io.InputStream
import java.sql.Connection

import anorm.SqlParser.{binaryStream, int, long}
import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.transaction.GlobalKey
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.ContractLifecycleEventsReader.ContractLifecycleEvent.{
  Archived,
  Created,
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.util.control.NonFatal

object ContractLifecycleEventsReader {
  def read(range: EventsRange[(Offset, Long)])(implicit
      conn: Connection
  ): Vector[ContractLifecycleEvent] =
    createsAndArchives(EventsRange(range.startExclusive._2, range.endInclusive._2), "ASC")
      .as(
        (offset("archived_at").? ~
          contractId("contract_id") ~
          binaryStream("create_key_value").? ~
          int("create_key_value_compression").? ~
          identifier("template_id") ~
          offset("created_at").? ~
          flatEventWitnessesColumn("flat_event_witnesses") ~
          long("event_sequential_id") ~
          int("kind")).map {
          case maybeArchivedAt ~ contractId ~ maybeCreateKeyValue ~ maybeCreateKeyValueCompression ~ templateId ~ maybeCreatedAt ~ flatEventWitnesses ~ eventSequentialId ~ kind =>
            val maybeGlobalKey =
              for {
                createKeyValue <- maybeCreateKeyValue
                createKeyValueCompression = Compression.Algorithm.assertLookup(
                  maybeCreateKeyValueCompression
                )
                keyValue = decompressAndDeserialize(createKeyValueCompression, createKeyValue)
              } yield GlobalKey.assertBuild(templateId, keyValue.value)
            val createdAt =
              maybeCreatedAt.getOrElse(
                throw new RuntimeException("Created at should not be missing")
              )

            if (kind == 20) {
              val archivedAt = maybeArchivedAt.getOrElse(
                throw new RuntimeException("Archived at should be present for consuming events")
              )
              Archived(
                archivedAt,
                contractId,
                maybeGlobalKey,
                flatEventWitnesses,
                createdAt,
                archivedAt,
                eventSequentialId,
              )
            } else
              Created(
                createdAt,
                contractId,
                maybeGlobalKey,
                flatEventWitnesses,
                createdAt,
                eventSequentialId,
              )
        }.*
      )
      .toVector

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) = {
    try {
      val _ = algorithm // TDT
      ValueSerializer.deserializeValue(Compression.Algorithm.GZIP.decompress(value))
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Failure Decompressing using $algorithm", e)
    }
  }

  private val createsAndArchives: (EventsRange[Long], String) => SimpleSql[Row] =
    (range: EventsRange[Long], limitExpr: String) => SQL"""
              SELECT
                archives.event_offset as archived_at,
                archives.contract_id as contract_id,
                creates.create_key_value as create_key_value,
                creates.template_id as template_id,
                creates.event_offset as created_at,
                archives.flat_event_witnesses as flat_event_witnesses,
                archives.event_sequential_id as event_sequential_id,
                20 as kind
              FROM participant_events creates
              LEFT JOIN participant_events archives
              ON archives.contract_id = creates.contract_id
              WHERE archives.event_sequential_id > ${range.startExclusive}
                    and archives.event_sequential_id <= ${range.endInclusive}
                    and archives.event_kind = 20 -- consuming
                    and creates.event_kind = 10 -- created
              UNION ALL
              SELECT
                null as archived_at,
                contract_id,
                create_key_value,
                template_id,
                event_offset as created_at,
                flat_event_witnesses,
                event_sequential_id,
                10 as kind
              FROM participant_events
              WHERE event_sequential_id > ${range.startExclusive}
                    and event_sequential_id <= ${range.endInclusive}
                    and event_kind = 10 -- created
              ORDER BY event_sequential_id #$limitExpr"""

  sealed trait ContractLifecycleEvent extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: Long
  }
  object ContractLifecycleEvent {
    final case class Created(
        createdAt: Offset,
        contractId: ContractId,
        globalKey: Option[GlobalKey],
        flatEventWitnesses: Set[Party],
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractLifecycleEvent
    final case class Archived(
        archivedAt: Offset,
        contractId: ContractId,
        globalKey: Option[GlobalKey],
        flatEventWitnesses: Set[Party],
        createdAt: Offset,
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractLifecycleEvent
  }

  case class ContractLifecycleResponse(events: Seq[ContractLifecycleEvent])
}
