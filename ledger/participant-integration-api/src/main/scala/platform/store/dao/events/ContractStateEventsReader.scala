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
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.util.control.NoStackTrace

object ContractStateEventsReader {
  type RawContractEvent = (
      ContractId,
      Option[Ref.Identifier],
      Option[InputStream],
      Option[Int],
      Option[InputStream],
      Option[Int],
      Long,
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
          identifier("template_id").? ~
          binaryStream("create_key_value").? ~
          int("create_key_value_compression").? ~
          binaryStream("create_argument").? ~
          int("create_argument_compression").? ~
          long("event_sequential_id") ~
          flatEventWitnessesColumn("flat_event_witnesses") ~
          int("event_kind") ~
          offset("event_offset")).map(SqlParser.flatten).*
      )
      .toVector

  def toContractStateEvent(
      row: RawContractEvent,
      lfValueTranslation: LfValueTranslation,
  ): ContractStateEvent =
    row match {
      case (
            contractId,
            templateId,
            maybeCreateKeyValue,
            maybeCreateKeyValueCompression,
            maybeCreateArgument,
            maybeCreateArgumentCompression,
            eventSequentialId,
            flatEventWitnesses,
            kind,
            offset,
          ) =>
        if (kind == 20) {
          Archived(
            contractId = contractId,
            stakeholders = flatEventWitnesses,
            eventOffset = offset,
            eventSequentialId = eventSequentialId,
          )
        } else {
          val (maybeGlobalKey: Option[GlobalKey], contract: Contract) = globalKeyAndContract(
            contractId,
            templateId.getOrElse(throw CreateMissingError("template_id")),
            maybeCreateKeyValue,
            maybeCreateKeyValueCompression,
            maybeCreateArgument.getOrElse(throw CreateMissingError("create_argument")),
            maybeCreateArgumentCompression,
            lfValueTranslation,
          )
          Created(
            contractId = contractId,
            contract = contract,
            globalKey =
              maybeGlobalKey, // TDT Look into using the retrieving the cached value for global key
            stakeholders = flatEventWitnesses,
            eventOffset = offset,
            eventSequentialId = eventSequentialId,
          )
        }
    }

  private def globalKeyAndContract(
      contractId: ContractId,
      templateId: events.Identifier,
      maybeCreateKeyValue: Option[InputStream],
      maybeCreateKeyValueCompression: Option[Int],
      createArgument: InputStream,
      maybeCreateArgumentCompression: Option[Int],
      lfValueTranslation: LfValueTranslation,
  ): (Option[Key], Contract) = {
    val maybeContractArgument =
      lfValueTranslation.cache.contracts
        .getIfPresent(LfValueTranslation.ContractCache.Key(contractId))
        .map(_.argument)
        .toRight(createArgument) // TDT

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
        templateId,
        createArgument = maybeContractArgument,
        createArgumentCompression =
          Compression.Algorithm.assertLookup(maybeCreateArgumentCompression),
      )
    (maybeGlobalKey, contract)
  }

  private[store] def toContract(
      templateId: Identifier,
      createArgument: Either[InputStream, events.Value],
      createArgumentCompression: Compression.Algorithm,
  ): Contract = {
    val deserialized =
      createArgument.fold(decompressAndDeserialize(createArgumentCompression, _), identity)

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
                contract_id,
                template_id,
                create_key_value,
                create_key_value_compression,
                create_argument,
                create_argument_compression,
                flat_event_witnesses,
                event_sequential_id,
                event_kind,
                event_offset
              FROM participant_events
              WHERE event_sequential_id > ${range.startExclusive}
                    and event_sequential_id <= ${range.endInclusive}
                    and (event_kind = 10 OR event_kind = 20) -- created or archived
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
        stakeholders: Set[Party],
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
    final case class Archived(
        contractId: ContractId,
        stakeholders: Set[Party],
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
    final case class LedgerEndMarker(
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
  }

  case class CreateMissingError(field: String) extends NoStackTrace {
    override def getMessage: String =
      s"Create events should not be missing $field"
  }
}
