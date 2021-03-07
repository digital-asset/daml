package com.daml.platform.store.dao.events

import java.io.InputStream
import java.sql.Connection

import anorm.SqlParser.{binaryStream, int, long}
import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.platform.store.Conversions.{contractId, offset, _}
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent.{
  Archived,
  Created,
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

object ContractStateEventsReader {
  type RawContractEvent = (
      ContractId,
      Ref.Identifier,
      Option[InputStream],
      Option[Int],
      Option[InputStream],
      Option[Int],
      Long,
      Option[Long],
      Set[Party],
      Int,
      Offset,
  )

  def read(range: EventsRange[(Offset, Long)])(implicit
      conn: Connection
  ): Vector[RawContractEvent] =
    createsAndArchives(EventsRange(range.startExclusive._2, range.endInclusive._2), "ASC")
      .as(
        (contractId("contract_id") ~
          identifier("template_id") ~
          binaryStream("create_key_value").? ~
          int("create_key_value_compression").? ~
          binaryStream("create_argument").? ~
          int("create_argument_compression").? ~
          long("created_at") ~
          long("archived_at").? ~
          flatEventWitnessesColumn("flat_event_witnesses") ~
          int("kind") ~
          offset("event_offset")).map(SqlParser.flatten).*
      )
      .toVector

  def toContractStateEvent(row: RawContractEvent): ContractStateEvent =
    row match {
      case (
            contractId,
            templateId,
            maybeCreateKeyValue,
            maybeCreateKeyValueCompression,
            maybeCreateArgument,
            maybeCreateArgumentCompression,
            createdAt,
            maybeArchivedAt,
            flatEventWitnesses,
            kind,
            offset,
          ) =>
        val maybeGlobalKey =
          for {
            createKeyValue <- maybeCreateKeyValue
            createKeyValueCompression = Compression.Algorithm.assertLookup(
              maybeCreateKeyValueCompression
            )
            keyValue = decompressAndDeserialize(createKeyValueCompression, createKeyValue)
          } yield GlobalKey.assertBuild(templateId, keyValue.value)
        val contract =
          toContract( // TDT Do not eagerly decode contract here (do it only for creates since it will only be needed for divulgence for deletes)
            contractId,
            templateId,
            createArgument = maybeCreateArgument.get,
            createArgumentCompression =
              Compression.Algorithm.assertLookup(maybeCreateArgumentCompression),
          )
        if (kind == 20) {
          val archivedAt = maybeArchivedAt.getOrElse(
            throw new RuntimeException("Archived at should be present for consuming exercises")
          )
          Archived(
            contractId = contractId,
            contract = contract,
            globalKey = maybeGlobalKey,
            flatEventWitnesses = flatEventWitnesses,
            createdAt = createdAt,
            eventOffset = offset,
            eventSequentialId = archivedAt,
          )
        } else
          Created(
            contractId = contractId,
            contract = contract,
            globalKey = maybeGlobalKey,
            flatEventWitnesses = flatEventWitnesses,
            eventOffset = offset,
            eventSequentialId = createdAt,
          )
    }

  private[store] def toContract(
      contractId: ContractId,
      templateId: Identifier,
      createArgument: InputStream,
      createArgumentCompression: Compression.Algorithm,
  ): Contract = {
    val decompressed = createArgumentCompression.decompress(createArgument)

    val deserialized = ValueSerializer.deserializeValue(
      stream = decompressed,
      errorContext = s"Failed to deserialize create argument for contract ${contractId.coid}",
    )
    Contract(
      template = templateId,
      arg = deserialized,
      agreementText = "",
    )
  }

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) =
    ValueSerializer.deserializeValue(algorithm.decompress(value))

  private val createsAndArchives: (EventsRange[Long], String) => SimpleSql[Row] =
    (range: EventsRange[Long], limitExpr: String) => SQL"""
              SELECT
                archives.contract_id as contract_id,
                creates.template_id as template_id,
                creates.create_key_value as create_key_value,
                creates.create_key_value_compression as create_key_value_compression,
                creates.create_argument as create_argument,
                creates.create_argument_compression as create_argument_compression,
                archives.flat_event_witnesses as flat_event_witnesses,
                archives.event_sequential_id as event_sequential_id,
                creates.event_sequential_id as created_at,
                archives.event_sequential_id as archived_at,
                20 as kind,
                archives.event_offset as event_offset
              FROM participant_events archives
              INNER JOIN participant_events creates -- TDT use LEFT JOIN in order to support prunning
              ON archives.contract_id = creates.contract_id
              WHERE archives.event_sequential_id > ${range.startExclusive}
                    and archives.event_sequential_id <= ${range.endInclusive}
                    and archives.event_kind = 20 -- consuming
                    and creates.event_kind = 10 -- created
              UNION ALL
              SELECT
                contract_id,
                template_id,
                create_key_value,
                create_key_value_compression,
                create_argument,
                create_argument_compression,
                flat_event_witnesses,
                event_sequential_id,
                event_sequential_id as created_at,
                null as archived_at,
                10 as kind,
                event_offset
              FROM participant_events
              WHERE event_sequential_id > ${range.startExclusive}
                    and event_sequential_id <= ${range.endInclusive}
                    and event_kind = 10 -- created
              ORDER BY event_sequential_id #$limitExpr"""

  sealed trait ContractStateEvent extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: Long
    def contract: Contract
    def flatEventWitnesses: Set[Party]
    def globalKey: Option[GlobalKey]
  }
  object ContractStateEvent {
    final case class Created(
        contractId: ContractId,
        contract: Contract,
        globalKey: Option[GlobalKey],
        flatEventWitnesses: Set[Party],
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
    final case class Archived(
        contractId: ContractId,
        contract: Contract,
        globalKey: Option[GlobalKey],
        flatEventWitnesses: Set[Party],
        createdAt: Long,
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
  }

  case class ContractLifecycleResponse(events: Seq[ContractStateEvent])
}
