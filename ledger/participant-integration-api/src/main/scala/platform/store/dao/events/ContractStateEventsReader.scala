package com.daml.platform.store.dao.events

import java.io.InputStream
import java.sql.Connection

import anorm.SqlParser.{binaryStream, int, long}
import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.platform.store.Conversions.{contractId, offset, _}
import com.daml.platform.store.dao.events
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent.{
  Archived,
  Created,
  Other,
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

object ContractStateEventsReader {
  type RawContractEvent = (
      Option[ContractId],
      Option[Ref.Identifier],
      Option[InputStream],
      Option[Int],
      Option[InputStream],
      Option[Int],
      Option[Long],
      Option[Long],
      Option[Set[Party]],
      Int,
      Long,
      Offset,
  )

  def read(range: EventsRange[(Offset, Long)])(implicit
      conn: Connection
  ): Vector[RawContractEvent] =
    createsAndArchives(EventsRange(range.startExclusive._2, range.endInclusive._2), "ASC")
      .as(
        (contractId("contract_id").? ~
          identifier("template_id").? ~
          binaryStream("create_key_value").? ~
          int("create_key_value_compression").? ~
          binaryStream("create_argument").? ~
          int("create_argument_compression").? ~
          long("created_at").? ~
          long("archived_at").? ~
          flatEventWitnessesColumn("flat_event_witnesses").? ~
          int("kind") ~
          long("event_sequential_id") ~
          offset("event_offset")).map(SqlParser.flatten).*
      )
      .toVector

  def toContractStateEvent(row: RawContractEvent): ContractStateEvent =
    row match {
      case (
            maybeContractId,
            maybeTemplateId,
            maybeCreateKeyValue,
            maybeCreateKeyValueCompression,
            maybeCreateArgument,
            maybeCreateArgumentCompression,
            maybeCreatedAt,
            maybeArchivedAt,
            maybeFlatEventWitnesses,
            kind,
            eventSequentialId,
            offset,
          ) =>
        if (kind == 20) {
          val (maybeGlobalKey: Option[GlobalKey], contract: Contract) =
            getGlobalKeyAndContract(
              maybeContractId.get,
              maybeTemplateId.get,
              maybeCreateKeyValue,
              maybeCreateKeyValueCompression,
              maybeCreateArgument.get,
              maybeCreateArgumentCompression,
            )
          Archived(
            contractId = maybeContractId.get,
            contract = contract,
            globalKey = maybeGlobalKey,
            flatEventWitnesses = maybeFlatEventWitnesses.get,
            createdAt = maybeCreatedAt.get,
            eventOffset = offset,
            eventSequentialId = maybeArchivedAt.get,
          )
        } else if (kind == 10) {
          val (maybeGlobalKey: Option[GlobalKey], contract: Contract) =
            getGlobalKeyAndContract(
              maybeContractId.get,
              maybeTemplateId.get,
              maybeCreateKeyValue,
              maybeCreateKeyValueCompression,
              maybeCreateArgument.get,
              maybeCreateArgumentCompression,
            )

          Created(
            contractId = maybeContractId.get,
            contract = contract,
            globalKey = maybeGlobalKey,
            flatEventWitnesses = maybeFlatEventWitnesses.get,
            eventOffset = offset,
            eventSequentialId = maybeCreatedAt.get,
          )
        } else
          Other(
            eventSequentialId = eventSequentialId,
            eventOffset = offset,
            kind = kind,
          )
    }

  private def getGlobalKeyAndContract(
      contractId: ContractId,
      templateId: events.Identifier,
      maybeCreateKeyValue: Option[InputStream],
      maybeCreateKeyValueCompression: Option[Int],
      createArgument: InputStream,
      maybeCreateArgumentCompression: Option[Int],
  ) = {
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
        createArgument = createArgument,
        createArgumentCompression =
          Compression.Algorithm.assertLookup(maybeCreateArgumentCompression),
      )
    (maybeGlobalKey, contract)
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
                archives.event_kind as kind,
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
                event_kind as kind,
                event_offset
              FROM participant_events
              WHERE event_sequential_id > ${range.startExclusive}
                    and event_sequential_id <= ${range.endInclusive}
                    and event_kind = 10 -- created
              UNION ALL
              SELECT
                null as contract_id,
                null as template_id,
                null as create_key_value,
                null as create_key_value_compression,
                null as create_argument,
                null as create_argument_compression,
                null as flat_event_witnesses,
                event_sequential_id,
                null as created_at,
                null as archived_at,
                event_kind as kind,
                event_offset
              FROM participant_events
              WHERE event_sequential_id > ${range.startExclusive}
                    and event_sequential_id <= ${range.endInclusive}
                    and event_kind <> 10
                    and event_kind <> 20 -- other events needed for having a complete stream
              ORDER BY event_sequential_id #$limitExpr"""

  sealed trait ContractStateEvent extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: Long
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
    final case class Other(
        eventOffset: Offset,
        eventSequentialId: Long,
        kind: Int,
    ) extends ContractStateEvent
  }

  case class ContractLifecycleResponse(events: Seq[ContractStateEvent])
}
